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

/**
 * RequestProcessors are chained together to process transactions. Requests are     RequestProcessors是处理事务的 processor链
 * always processed in order. The standalone server, follower, and leader all       server follower leader都有自己的 processor链
 * have slightly different RequestProcessors chained together.
 * 
 * Requests always move forward through the chain of RequestProcessors. Requests    调用processRequest()，将request传递到下一个processor
 * are passed to a RequestProcessor through processRequest(). Generally method       被一个线程执行
 * will always be invoked by a single thread.
 * 
 * When shutdown is called, the request RequestProcessor should also shutdown
 * any RequestProcessors that it is connected to.
 */
public interface RequestProcessor {
    @SuppressWarnings("serial")
    public static class RequestProcessorException extends Exception {
        public RequestProcessorException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    void processRequest(Request request) throws RequestProcessorException;

    void shutdown();
}
