/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.NIOServerCnxnFactory.SelectorThread;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class handles communication with clients using NIO. There is one per           和用户交互的类 每个client一个实例？只一个线程交互？
 * client, but only one thread doing the communication.
 */
public class NIOServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);

    private final NIOServerCnxnFactory factory;
    private final SocketChannel sock;
    private final SelectorThread selectorThread;
    private final SelectionKey sk;
    private boolean initialized;
    private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);          //保存长度
    private ByteBuffer incomingBuffer = lenBuffer;
    private final Queue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<ByteBuffer>();
    private int sessionTimeout;
    private final ZooKeeperServer zkServer;

    /**
     * The number of requests that have been submitted but not yet responded to.
     */
    private final AtomicInteger outstandingRequests = new AtomicInteger(0);

    /**
     * This is the id that uniquely identifies the session of a client. Once
     * this session is no longer active, the ephemeral nodes will go away.
     */
    private long sessionId;

    private final int outstandingLimit;

    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock, SelectionKey sk, NIOServerCnxnFactory factory, SelectorThread selectorThread) throws IOException {
        this.zkServer = zk;
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        this.selectorThread = selectorThread;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        if (zk != null) {
            outstandingLimit = zk.getGlobalOutstandingLimit();
        } else {
            outstandingLimit = 1;
        }
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not block */
        sock.socket().setSoLinger(false, -1);
        InetAddress addr = ((InetSocketAddress) sock.socket().getRemoteSocketAddress()).getAddress();
        authInfo.add(new Id("ip", addr.getHostAddress()));
        this.sessionTimeout = factory.sessionlessCnxnTimeout;
    }

    /* Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
       try {
           /* configure socket to be blocking
            * so that we dont have to do write in
            * a tight while loop
            */
           if (bb != ServerCnxnFactory.closeConn) {
               if (sock.isOpen()) {
                   sock.configureBlocking(true);
                   sock.write(bb);
               }
               packetSent();
           }
       } catch (IOException ie) {
           LOG.error("Error sending data synchronously ", ie);
       }
    }

    /**
     * sendBuffer pushes a byte buffer onto the outgoing buffer queue for
     * asynchronous writes.
     */
    public void sendBuffer(ByteBuffer bb) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Add a buffer to outgoingBuffers, sk " + sk + " is valid: " + sk.isValid());
        }
        outgoingBuffers.add(bb);
        requestInterestOpsUpdate();
    }

    /** Read the request payload (everything following the length prefix) */
    private void readPayload() throws IOException, InterruptedException {
        if (incomingBuffer.remaining() != 0) { // have we read length bytes?  未读满
            int rc = sock.read(incomingBuffer); // sock is non-blocking, so ok
            if (rc < 0) {
                throw new EndOfStreamException("Unable to read additional data from client sessionid 0x" + Long.toHexString(sessionId) + ", likely client has closed socket");
            }
        }

        if (incomingBuffer.remaining() == 0) { // have we read length bytes?   已读完len字节
            packetReceived();
            incomingBuffer.flip();
            if (!initialized) {                  //如果还没有初始化，是连接请求  连接不是accept线程就已经链接了吗？
                readConnectRequest();
            } else {                             //读取请求
                readRequest();
            }
            lenBuffer.clear();                  //果然，清空lenBuffer，然后incomingBuffer指向lenBuffer
            incomingBuffer = lenBuffer;
        }
    }

    /**
     * This boolean tracks whether the connection is ready for selection or           用于标志 当前连接是否准备好select
     * not. A connection is marked as not ready for selection while it is             当前连接正在处理IO请求时，被标记为没准备好select
     * processing an IO request. The flag is used to gatekeep pushing interest
     * op updates onto the selector.
     */
    private final AtomicBoolean selectable = new AtomicBoolean(true);

    public boolean isSelectable() {
        return sk.isValid() && selectable.get();
    }

    public void disableSelectable() {
        selectable.set(false);
    }

    public void enableSelectable() {
        selectable.set(true);
    }

    private void requestInterestOpsUpdate() {
        if (isSelectable()) {
            selectorThread.addInterestOpsUpdateRequest(sk);
        }
    }

    void handleWrite(SelectionKey k) throws IOException, CloseRequestException {
        if (outgoingBuffers.isEmpty()) {      // 如果为空，直接返回，那数据是先写到outgoingBuffers？
            return;
        }

        /*
         * This is going to reset the buffer position to 0 and the
         * limit to the size of the buffer, so that we can fill it
         * with data from the non-direct buffers that we need to
         * send.
         */
        ByteBuffer directBuffer = NIOServerCnxnFactory.getDirectBuffer();
        if (directBuffer == null) {
            ByteBuffer[] bufferList = new ByteBuffer[outgoingBuffers.size()];
            // Use gathered write call. This updates the positions of the
            // byte buffers to reflect the bytes that were written out.
            sock.write(outgoingBuffers.toArray(bufferList));

            // Remove the buffers that we have sent
            ByteBuffer bb;
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (bb.remaining() > 0) {      //检查当前数据是否发送完成，没发送完，break
                    break;
                }

                // bb已发送完成
                packetSent();
                outgoingBuffers.remove();    //移除掉已经发送完的
            }
         } else {
            directBuffer.clear();

            //先将足够多的 outgoingBuffers里面的数据复制到directBuffer
            for (ByteBuffer b : outgoingBuffers) {
                if (directBuffer.remaining() < b.remaining()) {     //如果directBuffer容纳不下b里面的可读数据
                    /*
                     * When we call put later, if the directBuffer is to
                     * small to hold everything, nothing will be copied,
                     * so we've got to slice the buffer if it's too big.
                     */
                    b = (ByteBuffer) b.slice().limit(directBuffer.remaining());  //slice一个新的buffer，并将新slice buffer的limit设置为directBuffer.remaining()
                }
                /*
                 * put() is going to modify the positions of both
                 * buffers, put we don't want to change the position of
                 * the source buffers (we'll do that after the send, if
                 * needed), so we save and reset the position after the
                 * copy
                 */
                int p = b.position(); //将之前的position记录下来
                directBuffer.put(b);
                b.position(p);        //恢复position
                if (directBuffer.remaining() == 0) {  //directBuffer写满了
                    break;
                }
            }
            /*
             * Do the flip: limit becomes position, position gets set to
             * 0. This sets us up for the write.
             */
            directBuffer.flip();

            int sent = sock.write(directBuffer);  //发送directBuffer里面的数据

            ByteBuffer bb;

            // Remove the buffers that we have sent
            // 移除掉已经发送的数据
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (sent < bb.remaining()) {      //如果发送的字节数目 小于 bb的可写字节数
                    /*
                     * We only partially sent this buffer, so we update
                     * the position and exit the loop.
                     */
                    bb.position(bb.position() + sent);      //则设置 bb的position
                    break;                                  //只有实际发送了之后才会设定position，这是对的
                }
                packetSent();
                /* We've sent the whole buffer, so drop the buffer */
                sent -= bb.remaining();                     // 减去已发送字节
                outgoingBuffers.remove();                   // 移除已发送buffer
            }
        }
    }

    /**
     * Only used in order to allow testing
     */
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    /**
     * Handles read/write IO on connection.
     */
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            if (isSocketOpen() == false) {
                LOG.warn("trying to do i/o on a null socket for session:0x" + Long.toHexString(sessionId));
                return;
            }
            if (k.isReadable()) {       // 可读
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    throw new EndOfStreamException("Unable to read additional data from client sessionid 0x" + Long.toHexString(sessionId) + ", likely client has closed socket");
                }
                if (incomingBuffer.remaining() == 0) {   // incomingBuffer读满了
                    boolean isPayload;                   // 读到的字节是否真的是长度信息
                    if (incomingBuffer == lenBuffer) {   // start of next request  刚开始创建NIOServerCnxn时，incomingBuffer和lenBuffer指向同一个对象
                        incomingBuffer.flip();                                  // 读完一个完成的请求后，incomingBuffer应该会再次指向lenBuffer？
                        isPayload = readLength(k);                              // incomingBuffer == lenBuffer时才去调用 readLength
                        incomingBuffer.clear();                                 // 清空incomingBuffer
                    } else {                   //如果不同的话，说明不是一个新请求
                        // continuation        //继续读取？command没有参数的嘛？
                        isPayload = true;
                    }
                    if (isPayload) { // not the case for 4letterword   如果不是command
                        readPayload();
                    }
                    else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }
            if (k.isWritable()) {
                handleWrite(k);

                if (!initialized && !getReadInterest() && !getWriteInterest()) {
                    throw new CloseRequestException("responded to info probe");
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x" + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            // expecting close to log session closure
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x" + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }

    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }

    // Only called as callback from zkServer.processPacket()
    protected void incrOutstandingRequests(RequestHeader h) {
        if (h.getXid() >= 0) {
            outstandingRequests.incrementAndGet();
            // check throttling
            int inProcess = zkServer.getInProcess();
            if (inProcess > outstandingLimit) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Throttling recv " + inProcess);
                }
                disableRecv();
            }
        }
    }

    // returns whether we are interested in writing, which is determined
    // by whether we have any pending buffers on the output queue or not
    // outgoingBuffers不为空，则对写感兴趣
    private boolean getWriteInterest() {
        return !outgoingBuffers.isEmpty();
    }

    // returns whether we are interested in taking new requests, which is
    // determined by whether we are currently throttled or not
    // 不进行流量控制，对读感兴趣
    private boolean getReadInterest() {
        return !throttled.get();
    }

    // false 表示不进行流量控制 对读感兴趣
    // true 表示进行流量控制    不对读感兴趣
    private final AtomicBoolean throttled = new AtomicBoolean(false);

    // Throttle acceptance of new requests. If this entailed a state change,
    // register an interest op update request with the selector.
    public void disableRecv() {
        if (throttled.compareAndSet(false, true)) {
            requestInterestOpsUpdate();
        }
    }

    // Disable throttling and resume acceptance of new requests. If this
    // entailed a state change, register an interest op update request with
    // the selector.
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            requestInterestOpsUpdate();
        }
    }

    //除了accept连接之外，每次客户端首次连接都会发送一个特定的包？所谓的connectRequest？但是其实和accept的概念不是一个？
    //建立连接之后会有一个首次的信息同步？
    private void readConnectRequest() throws IOException, InterruptedException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        zkServer.processConnectRequest(this, incomingBuffer);
        initialized = true;
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    private class SendBufferWriter extends Writer {   // chunk发送
        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            checkFlush(true);
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }

    /** Return if four letter word found and responded to, otw false **/
    // 如果是一个string command，返回true并回应
    // 否则返回false
    private boolean checkFourLetterWord(final SelectionKey k, final int len) throws IOException {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int

        if (!FourLetterCommands.isKnown(len)) {   // 如果len不在cmd2String的key中，则肯定不是一个string command，则返回false
            return false;
        }

        // 这儿没问题吗？如果len在cmd2String的key中，也不能百分百说明一定就是string command吧？
        // 可能只是接下来可读的字节真的正好是len呢？
        String cmd = FourLetterCommands.getCommandString(len);
        packetReceived();

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open.             netcat发送完数据后会立即关闭掉发送端，只开接收端，保持一个半连接
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch(Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

        final PrintWriter pwriter = new PrintWriter(new BufferedWriter(new SendBufferWriter()));

        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, this, cmd + " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing " + cmd + " command from " + sock.socket().getRemoteSocketAddress());

        if (len == FourLetterCommands.setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }
            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer, factory);
        }
    }

    /** Reads the first 4 bytes of lenBuffer, which could be true length or
     *  four letter word.
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     */
    // 返回 读到的是不是真的是长度信息
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer
        int len = lenBuffer.getInt();
        if (!initialized && checkFourLetterWord(sk, len)) {
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        incomingBuffer = ByteBuffer.allocate(len);   // 如果不是 string world，则分配一个len长度的buffer
        return true;
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }

    public long getOutstandingRequests() {
        return outstandingRequests.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Used by "dump" 4-letter command to list all connection in
     * cnxnExpiryMap
     */
    @Override
    public String toString() {
        return "ip: " + sock.socket().getRemoteSocketAddress() + " sessionId: 0x" + Long.toHexString(sessionId);
    }

    /**
     * Close the cnxn and remove it from the factory cnxns list.
     */
    @Override
    public void close() {
        if (!factory.removeCnxn(this)) {
            return;
        }

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }

        closeSock();
    }

    /**
     * Close resources associated with the sock of this cnxn.
     */
    private void closeSock() {
        if (sock.isOpen() == false) {
            return;
        }

        LOG.info("Closed socket connection for client " + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ? " which had sessionid 0x" + Long.toHexString(sessionId) : " (no session established for client)"));
        closeSock(sock);
    }

    /**
     * Close resources associated with a sock.
     */
    public static void closeSock(SocketChannel sock) {
        if (sock.isOpen() == false) {
            return;
        }

        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }

    private final static byte fourBytes[] = new byte[4];

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#sendResponse(org.apache.zookeeper.proto.ReplyHeader,
     *      org.apache.jute.Record, java.lang.String)
     */
    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Make space for length
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            try {
                baos.write(fourBytes);                    // fourBytes
                bos.writeRecord(h, "header");        // 头部
                if (r != null) {
                    bos.writeRecord(r, tag);              // body
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("Error serializing response");
            }
            byte b[] = baos.toByteArray();                 // 得到序列化之后的字节数组
            ByteBuffer bb = ByteBuffer.wrap(b);            // wrap之后的ByteBuffer，position为0
            bb.putInt(b.length - 4).rewind();              // 用长度信息填充前四个字节(fourBytes)
            sendBuffer(bb);                                // 异步发送
            if (h.getXid() > 0) {
                // check throttling
                //已经发送了一个，待发送的减一 或者还没有达到zkServer的上限
                if (outstandingRequests.decrementAndGet() < 1 || zkServer.getInProcess() < outstandingLimit) {
                    enableRecv();
                }
            }
         } catch(Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
         }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);   // 头部？
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK, "Deliver event " + event + " to 0x" + Long.toHexString(this.sessionId) + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();                        // 待处理的事件？

        sendResponse(h, e, "notification");
    }

    /*5
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
     */
    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        factory.touchCnxn(this);
    }

    @Override
    public int getInterestOps() {
        if (!isSelectable()) {
            return 0;
        }
        int interestOps = 0;
        if (getReadInterest()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (getWriteInterest()) {
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    public InetAddress getSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return sock.socket().getInetAddress();
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        throw new UnsupportedOperationException("SSL is unsupported in NIOServerCnxn");
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        throw new UnsupportedOperationException("SSL is unsupported in NIOServerCnxn");
    }

}
