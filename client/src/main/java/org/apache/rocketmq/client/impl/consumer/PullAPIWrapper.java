/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;

    // key：messageQueue value: long （推荐拉消息使用的主机id）
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 预处理 拉消息结果 。 主要将服务器端指定MessageQueue 的拉消息下一次的推荐节点  保存到 pullFromWhichNodeTable 中，
     * 以及消息客户端过滤
     *
     * @param mq               MessageQueue
     * @param pullResult       返回信息
     * @param subscriptionData 订阅数据
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
                                        final SubscriptionData subscriptionData) {
        // 扩展类
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        // 更新 updatePullFromWhichNodeTable 内 该mq的下次查询推荐 brokerId.
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            // 使用缓冲区 表示 messageBinary
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());

            //解码
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            List<MessageExt> msgListFilterAgain = msgList;

            // 客户端按照 tag过滤
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }
            // 客户端执行hook过滤
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                // 给消息增加三个property ：
                // 队列最小offset
                // 队列最大offset
                // 消息归属brokerName
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
            }
            // 将再次过滤后的消息list, 保存到pullRequest
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        // 将 pullResult的messageBinary 设置为null，help GC
        pullResultExt.setMessageBinary(null);
        // 返回预处理完的 pullResult
        return pullResult;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    // pullAPIWrapper.pullKernelImpl(...)
    // 拉消息信息代码
    // 参数1. messageQueue, 拉消息队列
    // 参数2. subExpression, 过滤表达式， 一般是null
    // 参数3. 表达式类型， 一根是tag
    // 参数4. 客户端版本
    // 参数5. nextOffset, 本次拉消息offset (重要)
    // 参数6. pullBatchSize, 拉消息最多消费条数限制
    // 参数7. sysFlag
    // 参数8. commitOffsetValue， 消费者本地该队列的消费进度
    // 参数9. BROKER_SUSPEND_MAX_TIME_MILLIS, 控制服务器端轮训时， 最长 hold 的时间 （15秒）
    // 参数10. CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, 网络调用超时时间限制（30秒）
    // 参数11. CommunicationMode.ASYNC, RPC调用模式， 使用的是异步模式
    // 参数12. pullBack, 拉消息结果回调处理对象
    public PullResult pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        // 查询指定brokerName的地址信息
        // 参数1. brokerName
        // 参数2. this.recalculatePullFromWhichNode(mq)  （可能0，也可能 1）
        // 参数3. false
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(
                mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq),
                false
        );

        if (null == findBrokerResult) {
            // 到nameserver 获取指定topic 的路由数据， 路由数据包含 主机信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                        && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    // RMQ 在 4.1.0 之后才支持的 非tag过滤
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                            + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            // 条件成立， 说明findBrokerResult 表示的主机为slave 节点。 slave 不存储offset信息
            if (findBrokerResult.isSlave()) {
                // 将 sysFlag 标记位中 commitOffset的位， 设置为0
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            // 创建header对象。 并且初始化值， 将业务参数全部封装进去
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            // 获取broker addr
            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            // 调用api.去 拉取消息

            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                    brokerAddr,
                    requestHeader,
                    timeoutMillis,
                    communicationMode,
                    pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
