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

import java.io.IOException;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.DataTreeBean;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * 
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: PrepRequestProcessor -> ProposalRequestProcessor ->
 * CommitProcessor -> Leader.ToBeAppliedRequestProcessor ->
 * FinalRequestProcessor
 */
public class LeaderZooKeeperServer extends QuorumZooKeeperServer {
    CommitProcessor commitProcessor;

    /**
     * @param port
     * @param dataDir
     * @throws IOException
     */
    LeaderZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout,
                self.maxSessionTimeout, treeBuilder, zkDb, self);
    }

    public Leader getLeader(){
        return self.leader;
    }
    
    @Override
    protected void setupRequestProcessors() {
        /**
         * 处理一些善后的工作
         */
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(
                finalProcessor, getLeader().toBeApplied);

        /**
         * 把请求commit应用到自己的内存数据库中去
         * 接着可以发送commit请求给所有的follower，让follower也commit到自己的内存数据库里去
         * 子要进去内存数据库，其他人就能弄看到；
         */
        commitProcessor = new CommitProcessor(toBeAppliedProcessor,
                Long.toString(getServerId()), false);
        commitProcessor.start();

        /**
         * 必然是把请求从outstandingQueue中获取出来，然后，负责进行2pc的同步；
         * 1、将proposal写入本地事务日志
         * 2、将proposal发送给所有的follower，所有的follower也得写入自己的本地事务日志里去
         * 3、等待过半的follower对这个请求返回ack
         */
        ProposalRequestProcessor proposalProcessor = new ProposalRequestProcessor(this,
                commitProcessor);
        proposalProcessor.initialize();

        /**
         * 对请求做了一定的封装，并放入队列中
         */
        firstProcessor = new PrepRequestProcessor(this, proposalProcessor);
        ((PrepRequestProcessor)firstProcessor).start();
    }

    @Override
    public int getGlobalOutstandingLimit() {
        return super.getGlobalOutstandingLimit() / (self.getQuorumSize() - 1);
    }
    
    @Override
    public void createSessionTracker() {
        sessionTracker = new SessionTrackerImpl(this, getZKDatabase().getSessionWithTimeOuts(),
                tickTime, self.getId());
    }
    
    @Override
    protected void startSessionTracker() {
        ((SessionTrackerImpl)sessionTracker).start();
    }


    public boolean touch(long sess, int to) {
        return sessionTracker.touchSession(sess, to);
    }

    @Override
    protected void registerJMX() {
        // register with JMX
        try {
            jmxDataTreeBean = new DataTreeBean(getZKDatabase().getDataTree());
            MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxDataTreeBean = null;
        }
    }

    public void registerJMX(LeaderBean leaderBean,
            LocalPeerBean localPeerBean)
    {
        // register with JMX
        if (self.jmxLeaderElectionBean != null) {
            try {
                MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }

        try {
            jmxServerBean = leaderBean;
            MBeanRegistry.getInstance().register(leaderBean, localPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    @Override
    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxDataTreeBean = null;
    }

    protected void unregisterJMX(Leader leader) {
        // unregister from JMX
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
    }
    
    @Override
    public String getState() {
        return "leader";
    }

    /**
     * Returns the id of the associated QuorumPeer, which will do for a unique
     * id of this server. 
     */
    @Override
    public long getServerId() {
        return self.getId();
    }    
    
    @Override
    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
        int sessionTimeout) throws IOException {
        super.revalidateSession(cnxn, sessionId, sessionTimeout);
        try {
            // setowner as the leader itself, unless updated
            // via the follower handlers
            setOwner(sessionId, ServerCnxn.me);
        } catch (SessionExpiredException e) {
            // this is ok, it just means that the session revalidation failed.
        }
    }
}
