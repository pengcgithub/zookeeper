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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl extends Thread implements SessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    HashMap<Long, SessionImpl> sessionsById = new HashMap<Long, SessionImpl>();

    HashMap<Long, SessionSet> sessionSets = new HashMap<Long, SessionSet>();

    ConcurrentHashMap<Long, Integer> sessionsWithTimeout;
    long nextSessionId = 0;
    long nextExpirationTime;

    int expirationInterval;

    public static class SessionImpl implements Session {
        SessionImpl(long sessionId, int timeout, long expireTime) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            // 下一次session会过期的时间（tickTime = sessionid + timeout）
            this.tickTime = expireTime;
            isClosing = false;
        }

        final long sessionId;
        final int timeout;
        long tickTime;
        boolean isClosing;

        Object owner;

        public long getSessionId() { return sessionId; }
        public int getTimeout() { return timeout; }
        public boolean isClosing() { return isClosing; }
    }

    public static long initializeNextSession(long id) {
        long nextSid = 0;
        /**
         * 1、获取当前时间的毫秒表示。我们假设System.currentTimeMillis()取出的值是1380895182327，其64位二进制表示是：0000000000000000000000 010100000110000011110001000100110111110111 其中红色部分表示高24位，下划线部分表示低40位。
         *
         * 2、左移24位。将步骤1中的数值左移24位，得到如下二进制表示的数值：0100000110000011110001000100110111110111000000000000000000000000
         *
         * 3、无符号右移8位。再将步骤2中的数值无符号右移8位，得到如下二进制表示的数值：0000000001000001100000111100010001001101111101110000000000000000
         *
         * 4、添加机器标识SID.在initializeNextSession方法中，出现了一个id变量，该变量就是当前Zookeeper服务器的SID值.SID就是当时配置在myid文件中的值，该值通常是一个整数。我们以2为例子。2的64位表示如下：0000000000000000000000000000000000000000000000000000000000000010。高56位都是0，将其左移56位，可以得到如下二进制表示的数值：0000001000000000000000000000000000000000000000000000000000000000
         *
         * 5、将步骤3和步骤4得到的64位数值进行“|”操作：0000001001000001100000111100010001001101111101110000000000000000。这样就完成了一个sessioID的初始化。我们就可以得到一个单机唯一的序列号。算法概括为：高8位确定了所在机器，后56位使用当前时间的毫秒表示进行随机。
         */
        nextSid = (System.currentTimeMillis() << 24) >>> 8;
        nextSid =  nextSid | (id <<56);
        return nextSid;
    }

    static class SessionSet {
        HashSet<SessionImpl> sessions = new HashSet<SessionImpl>();
    }

    SessionExpirer expirer;

    private long roundToInterval(long time) {
        // We give a one interval grace period
        // 默认 expirationInterval = 3000

        /**
         * time = now() + timeout
         * timeout(默认) = 3000 * 2
         * expirationInterval(默认) = 3000
         *
         * 2022-04-13 15:59:54 + timeout = 1649836800000
         * 2022-04-13 15:59:55 + timeout = 1649836801000
         *
         * (1649836800000 / 3000 + 1) * 3000 = 1649836803000
         * (1649836801000 / 3000 + 1) * 3000 = 1649836803000
         *
         * 由于除数取整原因，所以最终不同的时间，算出来的结果是一样的；
         *
         */
        return (time / expirationInterval + 1) * expirationInterval;
    }

    public SessionTrackerImpl(SessionExpirer expirer,
            ConcurrentHashMap<Long, Integer> sessionsWithTimeout, int tickTime,
            long sid)
    {
        super("SessionTracker");
        this.expirer = expirer;
        this.expirationInterval = tickTime;
        this.sessionsWithTimeout = sessionsWithTimeout;
        nextExpirationTime = roundToInterval(System.currentTimeMillis());
        // sid = myid
        this.nextSessionId = initializeNextSession(sid);
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            addSession(e.getKey(), e.getValue());
        }
    }

    volatile boolean running = true;

    volatile long currentTime;

    synchronized public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session Sets (");
        pwriter.print(sessionSets.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(sessionSets.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            pwriter.print(sessionSets.get(time).sessions.size());
            pwriter.print(" expire at ");
            pwriter.print(new Date(time));
            pwriter.println(":");
            for (SessionImpl s : sessionSets.get(time).sessions) {
                pwriter.print("\t0x");
                pwriter.println(Long.toHexString(s.sessionId));
            }
        }
    }

    @Override
    synchronized public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    @Override
    synchronized public void run() {
        try {
            while (running) {
                currentTime = System.currentTimeMillis();
                // nextExpirationTime在SessionTrackImpl被构造时候就已经初始化了，主要根据server端设置的tickTime计算出来的，是tickTime的整数倍
                // nextExpirationTime > currentTime说明还没有到过期检测时间点，等待一定时间；
                if (nextExpirationTime > currentTime) {
                    this.wait(nextExpirationTime - currentTime);
                    continue;
                }

                // 执行到当前，说明nextExpirationTime对应的Session已经过期了，所以直接执行expire方法
                SessionSet set;
                set = sessionSets.remove(nextExpirationTime);
                if (set != null) {
                    for (SessionImpl s : set.sessions) {
                        // 修改Session状态，并向客户端发送expire请求
                        setSessionClosing(s.sessionId);
                        expirer.expire(s);
                    }
                }
                // 重新设置下次过期检测时间点nextExpirationTime
                nextExpirationTime += expirationInterval;
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected interruption", e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    synchronized public boolean touchSession(long sessionId, int timeout) {
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.CLIENT_PING_TRACE_MASK,
                                     "SessionTrackerImpl --- Touch session: 0x"
                    + Long.toHexString(sessionId) + " with timeout " + timeout);
        }
        SessionImpl s = sessionsById.get(sessionId);
        // Return false, if the session doesn't exists or marked as closing
        if (s == null || s.isClosing()) {
            return false;
        }
        // session下一次过期的时间
        long expireTime = roundToInterval(System.currentTimeMillis() + timeout);
        if (s.tickTime >= expireTime) {
            // Nothing needs to be done
            return true;
        }

        // 获取上一轮的过期时间，然后根据上一轮的过期时间移除对应的分桶中的session会话
        SessionSet set = sessionSets.get(s.tickTime);
        if (set != null) {
            set.sessions.remove(s);
        }

        // 新的过期时间赋值给tickTime
        s.tickTime = expireTime;
        set = sessionSets.get(s.tickTime);
        if (set == null) {
            set = new SessionSet();
            sessionSets.put(expireTime, set);
        }
        set.sessions.add(s);
        return true;
    }

    synchronized public void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.info("Session closing: 0x" + Long.toHexString(sessionId));
        }
        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        s.isClosing = true;
    }

    synchronized public void removeSession(long sessionId) {
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Removing session 0x"
                    + Long.toHexString(sessionId));
        }
        if (s != null) {
            SessionSet set = sessionSets.get(s.tickTime);
            // Session expiration has been removing the sessions   
            if(set != null){
                set.sessions.remove(s);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                                     "Shutdown SessionTrackerImpl!");
        }
    }


    synchronized public long createSession(int sessionTimeout) {
        /**
         * 1、生成一个唯一的sessionID
         * 2、在几个内存数据结构中放入session
         * 3、对session计算他的过期时间以及进行特殊的处理
         */
        addSession(nextSessionId, sessionTimeout);

        //每次分配完session之后都会增加1
        return nextSessionId++;
    }

    synchronized public void addSession(long id, int sessionTimeout) {
        // 存放session的过期时间的
        sessionsWithTimeout.put(id, sessionTimeout);
        if (sessionsById.get(id) == null) {
            SessionImpl s = new SessionImpl(id, sessionTimeout, 0);
            sessionsById.put(id, s);
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Adding session 0x"
                        + Long.toHexString(id) + " " + sessionTimeout);
            }
        } else {
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Existing session 0x"
                        + Long.toHexString(id) + " " + sessionTimeout);
            }
        }
        // 分桶管理
        touchSession(id, sessionTimeout);
    }

    synchronized public void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        SessionImpl session = sessionsById.get(sessionId);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    synchronized public void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }
}
