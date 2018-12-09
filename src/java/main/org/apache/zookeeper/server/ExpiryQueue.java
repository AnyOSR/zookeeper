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

import org.apache.zookeeper.common.Time;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.               时间排序固定持续时间的bucket？
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 */
public class ExpiryQueue<E> {
    private final ConcurrentHashMap<E, Long> elemMap = new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,      最大bucket数目是 超时时间/时间间隔 (总时间/每一段时间)
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     */
    // 过期时刻 --- 过期时刻是key的元素
    private final ConcurrentHashMap<Long, Set<E>> expiryMap = new ConcurrentHashMap<Long, Set<E>>();    // key 应该是经过映射的
                                                                                                        // 一个周期内的(expirationInterval)时间段内 映射到同一个点
    private final AtomicLong nextExpirationTime = new AtomicLong();
    private final int expirationInterval;     // 每一小段时间间隔区间

    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    // 将当前time对齐到expirationInterval，然后加上expirationInterval
    // 返回一个加上expirationInterval的时刻
    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     * @param elem  element to remove
     * @return      time at which the element was set to expire, or null if
     *              it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);      //对上了
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     * @param elem     element to add/update
     * @param timeout  timout in milliseconds
     * @return         time at which the element is now set to expire if
     *                 changed, or null if unchanged
     */
    public Long update(E elem, int timeout) {
        Long prevExpiryTime = elemMap.get(elem);                        // 获得之前的ExpiryTime
        long now = Time.currentElapsedTime();                           // 当前时刻的流逝时间
        Long newExpiryTime = roundToNextInterval(now + timeout);   // 计算新的超时时间

        if (newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        // First add the elem to the new expiry time bucket in expiryMap.
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(elem);                                                         // 将当前元素添加到 newExpiryTime对应的set集合中

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        prevExpiryTime = elemMap.put(elem, newExpiryTime);                      // 更新elem的过期时间
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {  // elem之前就存在，且新的过期时间不等于之前的过期时间
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                prevSet.remove(elem);                                           //将elem从prevExpiryTime对应的set中删除
            }
        }
        return newExpiryTime;                                                   //返回新的过期时间
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     *         ready
     */
    //返回过期的数据
    //存在一个问题，当前方法不能返回全部的已经过期的E
    //只能返回一个set<E>
    //外层有一个for循环？
    public Set<E> poll() {
        long now = Time.currentElapsedTime();                  //当前流逝时间
        long expirationTime = nextExpirationTime.get();        //下次超时时间
        if (now < expirationTime) {                            //还没有到expirationTime，直接返回
            return Collections.emptySet();
        }

        //当前时间已经过了nextExpirationTime
        Set<E> set = null;
        long newExpirationTime = expirationTime + expirationInterval;   //计算下次超时时间 每次在nextExpirationTime的基础上加上expirationInterval
        if (nextExpirationTime.compareAndSet(expirationTime, newExpirationTime)) {  //如果成功更新
            set = expiryMap.remove(expirationTime);                                                // 删除掉expirationTime对应的连接数据
        }
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -> elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }
}

