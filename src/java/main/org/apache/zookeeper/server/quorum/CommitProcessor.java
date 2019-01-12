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

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This RequestProcessor matches the incoming committed requests with the               匹配 incoming committed requests
 * locally submitted requests. The trick is that locally submitted requests that             locally submitted requests
 * change the state of the system will come back as incoming committed requests,        本地提交的请求 will come back as incoming committed requests
 * so we need to match them up.                                                         so match
 *
 * The CommitProcessor is multi-threaded. Communication between threads is              多线程   通过队列 atomics 同步来进行通信
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to           作为一个网关，允许请求继续在处理链上进行处理
 * continue with the remainder of the processing pipeline. It will allow many          同时允许许多读请求，但是只有一个写请求
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.             保证写请求 顺序处理
 *
 *   - 1   commit processor main thread, which watches the request queues and           主线程 观察请求队列，基于sessionId分发请求到工作线程，
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned          so，对于某一个特定的session来说，读写任务总是在一个线程上被处理 保证顺序
 *         to the same thread (and hence are guaranteed to run in order).
 *   - 0-N worker threads, which run the rest of the request processor pipeline         工作线程 剩下processor request的处理线程
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 *
 * Multi-threading constraints:                                                       多线程约束：
 *   - Each session's requests must be processed in order.                                          每个session的请求必须顺序处理
 *   - Write requests must be processed in zxid order                                               写请求必须按照zxid顺序处理
 *   - Must ensure no race condition between writes in one session that would                       避免竞态条件
 *     trigger a watch being set by a read request in another session
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 *
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS = "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT = "zookeeper.commitProcessor.shutdownTimeout";

    /**
     * Requests that we are holding until the commit comes in.
     */
    protected final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Requests that have been committed.
     */
    protected final LinkedBlockingQueue<Request> committedRequests = new LinkedBlockingQueue<Request>();

    /** Request for which we are currently awaiting a commit */
    protected final AtomicReference<Request> nextPending = new AtomicReference<Request>();
    /** Request currently being committed (ie, sent off to next processor) */
    private final AtomicReference<Request> currentlyCommitting = new AtomicReference<Request>();   // 初始化的引用为null

    /** The number of requests currently being processed */  //正在被后续processor处理的request
    protected AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    RequestProcessor nextProcessor;

    protected volatile boolean stopped = true;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;

    /**
     * This flag indicates whether we need to wait for a response to come back from the   是否需要等待从leader返回的响应
     * leader or we just let the sync operation flow through like a read. The flag will   或者让同步操作直接流过，像读操作一样
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id, boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    private boolean isWaitingForCommit() {
        return nextPending.get() != null;
    }

    private boolean isProcessingCommit() {
        return currentlyCommitting.get() != null;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setData:
            case OpCode.reconfig:
            case OpCode.multi:
            case OpCode.setACL:
                return true;
            case OpCode.sync:
                return matchSyncs;    
            case OpCode.createSession:
            case OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    // 主线程
    @Override
    public void run() {
        Request request;
        try {
            while (!stopped) {
                synchronized(this) {
                    while (!stopped &&
                        ((queuedRequests.isEmpty() || isWaitingForCommit() || isProcessingCommit()) && (committedRequests.isEmpty() || isProcessingRequest()))) {
                        wait();
                    }
                }
                // 如果queuedRequests不为空且没有在等待commit且没有在处理commit(没有在处理写请求)  入口
                // 或者committedRequests不为空且 没有在处理request请求(没有在处理读请求)  状态机

                /*
                 * Processing queuedRequests: Process the next requests until we
                 * find one for which we need to wait for a commit. We cannot
                 * process a read request while we are processing write request.
                 */
                //  没有在等待commit 且没有commit请求正在被处理，则处理queuedRequests里面的请求
                // 如果正在wait commit 或者正在处理commit，则不处理queuedRequests里面的请求
                // 读写都是由主线程发起的
                while (!stopped && !isWaitingForCommit() && !isProcessingCommit() && (request = queuedRequests.poll()) != null) {
                    if (needCommit(request)) {       // 需要commit，则将nextPending指向这个request  写 主线程
                        nextPending.set(request);
                    } else {                        // 否则不需要commit，直接传递到下一个processor进行处理，读 worker线程
                        sendToNextProcessor(request);
                    }
                }

                /*
                 * Processing committedRequests: check and see if the commit
                 * came in for the pending request. We can only commit a
                 * request when there is no other request being processed.
                 */
                // 如果正在等待commit或者正在处理commit
                processCommitted();
            }
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    /*
     * Separated this method from the main run loop
     * for test purposes (ZOOKEEPER-1863)
     */
    protected void processCommitted() {
        Request request;

        // 处理commit的时候，没有request正在处理
        // 互斥
        if (!stopped && !isProcessingRequest() && (committedRequests.peek() != null)) {

            /*
             * ZOOKEEPER-1863: continue only if there is no new request
             * waiting in queuedRequests or it is waiting for a
             * commit. 
             */
            if ( !isWaitingForCommit() && !queuedRequests.isEmpty()) {
                return;
            }
            request = committedRequests.poll();

            /*
             * We match with nextPending so that we can move to the
             * next request when it is committed. We also want to
             * use nextPending because it has the cnxn member set
             * properly.
             */
            Request pending = nextPending.get();
            if (pending != null && pending.sessionId == request.sessionId && pending.cxid == request.cxid) {
                // we want to send our version of the request.
                // the pointer to the connection in the request
                pending.setHdr(request.getHdr());
                pending.setTxn(request.getTxn());
                pending.zxid = request.zxid;
                // Set currentlyCommitting so we will block until this
                // completes. Cleared by CommitWorkRequest after
                // nextProcessor returns.
                currentlyCommitting.set(pending);
                nextPending.set(null);
                sendToNextProcessor(pending);
            } else {
                // this request came from someone else so just
                // send the commit packet
                currentlyCommitting.set(request);
                sendToNextProcessor(request);
            }
        }      
    }

    @Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring CommitProcessor with " + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads.");
        if (workerPool == null) {
            workerPool = new WorkerService("CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();                                      // 增大 numRequestsProcessing,表示有request正在被处理
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor," + " unable to continue.");
                CommitProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            try {
                nextProcessor.processRequest(request);
            } finally {
                // If this request is the commit request that was blocking
                // the processor, clear.
                currentlyCommitting.compareAndSet(request, null);

                /*
                 * Decrement outstanding request count. The processor may be
                 * blocked at the moment because it is waiting for the pipeline
                 * to drain. In that case, wake it up if there are pending
                 * requests.
                 */
                if (numRequestsProcessing.decrementAndGet() == 0) {
                    if (!queuedRequests.isEmpty() || !committedRequests.isEmpty()) {
                        wakeup();
                    }
                }
            }
        }
    }

    synchronized private void wakeup() {
        notifyAll();
    }

    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing request:: " + request);
        }
        committedRequests.add(request);
        if (!isProcessingCommit()) {
            wakeup();
        }
    }

    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        queuedRequests.add(request);
        if (!isWaitingForCommit()) {
            wakeup();
        }
    }

    private void halt() {
        stopped = true;
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
