/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.internals.Fetcher.hasUsableOffsetForLeaderEpochVersion;

/**
 * 用于跟踪用户的主题、分区和偏移量的类。
 * 通过{@link #assignFromUser(Set)}(手动分配)或{@link #assignFromSubscribed(Collection)}(从订阅中自动分配)直接“分配”分区。
 *
 * 一旦分配了分区，直到它的初始位置被{@link #seekValidated(TopicPartition, FetchPosition)}设置， 它才被认为是“可取”的。
 * 可取分区跟踪一个用于设置下一次取的偏移量的取值位置，以及一个被消耗的位置，它是返回给用户的最后一个偏移量。
 * 您可以通过{@link #pause(TopicPartition)}挂起从分区的抓取，而不影响获取/消耗的偏移量。
 * 在使用{@link #resume(TopicPartition)}之前，分区将保持不可取。
 * 您还可以使用{@link #isPaused(TopicPartition)}独立地查询暂停状态。
 *
 * 注意，当分区分配被用户直接更改或通过组重新平衡更改时，暂停状态以及获取/使用位置不会保留。
 *
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seekValidated(TopicPartition, FetchPosition)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * Thread Safety: this class is thread-safe.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

    /**
     * 订阅状态枚举
     */
    private enum SubscriptionType {
        /**
         * SubscriptionState.subscriptionType的初始值
         */
        NONE,
        /**
         * 按照指定的topic名字进行订阅，自动分配分区
         */
        AUTO_TOPICS,
        /**
         * 按照指定的正则表达式匹配topic进行订阅，自动分配分区
         */
        AUTO_PATTERN,
        /**
         * 用户手动指定消费者消费的topic以及分区编号
         */
        USER_ASSIGNED
    }

    /* the type of subscription */
    /**
     * 订阅状态
     */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    /**
     * 订阅正则
     */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    /**
     * 订阅的topic set
     */
    private Set<String> subscription;

    /**
     * 该组已订阅的主题列表。
     * 这可能包括一些不属于group leader“订阅”的主题，因为它负责检测元数据的变化，这group rebalance。
     * <p>
     * The list of topics the group has subscribed to. This may include some topics which are not part
     * of `subscription` for the leader of a group since it is responsible for detecting metadata changes
     * which require a group rebalance.
     */
    private Set<String> groupSubscription;

    /** the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    /** 当前分配的分区及分区状态 */
    private final PartitionStates<TopicPartitionState> assignment;

    /** Default offset reset strategy */
    /** 默认offset重置策略 */
    private final OffsetResetStrategy defaultResetStrategy;

    /** User-provided listener to be invoked when assignment changes */
    /**
     * consumer rebalance 监听器
     */
    private ConsumerRebalanceListener rebalanceListener;

    private int assignmentId = 0;

    @Override
    public synchronized String toString() {
        return "SubscriptionState{" +
                "type=" + subscriptionType +
                ", subscribedPattern=" + subscribedPattern +
                ", subscription=" + String.join(",", subscription) +
                ", groupSubscription=" + String.join(",", groupSubscription) +
                ", defaultResetStrategy=" + defaultResetStrategy +
                ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
    }

    /**
     * 获取主题订阅柜规则
     */
    public synchronized String prettyString() {
        switch (subscriptionType) {
            case NONE:
                return "None";
            case AUTO_TOPICS:
                return "Subscribe(" + String.join(",", subscription) + ")";
            case AUTO_PATTERN:
                return "Subscribe(" + subscribedPattern + ")";
            case USER_ASSIGNED:
                return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
            default:
                throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
        }
    }

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * 单调递增的id，每次赋值变化后递增。
     * 当赋值发生变化时，可以用它来检查。
     * Monotonically increasing id which is incremented after every assignment change. This can
     * be used to check when an assignment has changed.
     *
     * @return The current assignment Id
     */
    synchronized int assignmentId() {
        return assignmentId;
    }

    /**
     * 如果没有设置订阅类型时，设置类型，或者验证订阅类型是否为给定类型
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     *
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE) {
            this.subscriptionType = type;
        } else if (this.subscriptionType != type) {
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        }
    }

    /**
     * 设置订阅topic和rebalance listener
     */
    public synchronized boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        return changeSubscription(topics);
    }

    /**
     * 设置订阅正则和rebalance listener
     */
    public synchronized void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_PATTERN);
        this.subscribedPattern = pattern;
    }

    /**
     * 当订阅类型是正则时，修改订阅的topic
     */
    public synchronized boolean subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN) {
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);
        }

        return changeSubscription(topics);
    }

    /**
     * 修改订阅主题
     */
    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe)) {
            return false;
        }

        subscription = topicsToSubscribe;
        return true;
    }

    /**
     * 设置当前group订阅的topic。group leader使用它来确保接收组所订阅的所有主题的元数据更新。
     * <p>
     * Set the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     *
     * @param topics All topics from the group subscription
     * @return true if the group subscription contains topics which are not part of the local subscription
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        if (!hasAutoAssignedPartitions()) {
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        }
        groupSubscription = new HashSet<>(topics);
        return !subscription.containsAll(groupSubscription);
    }

    /**
     * 重置group subscription 为只包含该消费者订阅的主题。
     * 设置groupSubscription为空集合
     * <p>
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    synchronized void resetGroupSubscription() {
        groupSubscription = Collections.emptySet();
    }

    /**
     * 更改为用户提供的指定分区的分配，
     * 注意这与{@link #assignFromSubscribed(Collection)}不同，{@link #assignFromSubscribed(Collection)}的输入分区是由订阅的主题提供的。
     * <p>
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public synchronized boolean assignFromUser(Set<TopicPartition> partitions) {
        // 设置订阅类型为用户指定
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        // 检查是否相同
        if (this.assignment.partitionSet().equals(partitions)) {
            return false;
        }
        // 累加
        assignmentId++;

        // update the subscribed topics
        // 组装数据
        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null) {
                state = new TopicPartitionState();
            }
            partitionToState.put(partition, state);

            manualSubscribedTopics.add(partition.topic());
        }
        // 设置当前分配的分区及分区状态
        this.assignment.set(partitionToState);
        // 修改订阅主题
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * 检查是否订阅了主题
     *
     * @return true if assignments matches subscription, otherwise false
     */
    public synchronized boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        for (TopicPartition topicPartition : assignments) {
            if (this.subscribedPattern != null) {
                // 检查是否匹配正则
                if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                            topicPartition,
                            this.subscribedPattern);

                    return false;
                }
            } else {
                // 检查是否包含topic
                if (!this.subscription.contains(topicPartition.topic())) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 将分配更改为从coordinator返回的指定分区，
     * 这与{@link #assignFromUser(Set)}不同，后者使用用户设置。
     * <p>
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     */
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.hasAutoAssignedPartitions()) {
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");
        }

        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            // 从已经缓存的数据中获取状态
            TopicPartitionState state = this.assignment.stateValue(tp);
            if (state == null) {
                state = new TopicPartitionState();
            }
            assignedPartitionStates.put(tp, state);
        }

        assignmentId++;
        // 设置assignment
        this.assignment.set(assignedPartitionStates);
    }

    /**
     * 设置监听器
     *
     * @param listener
     */
    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        }
        this.rebalanceListener = listener;
    }

    /**
     * 检查是否为正则模式
     * Check whether pattern subscription is in use.
     */
    synchronized boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    /**
     * 检查是否未设置订阅模式
     *
     * @return
     */
    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    /**
     * 注销
     */
    public synchronized void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.groupSubscription = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        this.assignmentId++;
    }

    /**
     * 检查主题是否与订阅的模式匹配。
     * <p>
     * Check whether a topic matches a subscribed pattern.
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern, false otherwise
     */
    synchronized boolean matchesSubscribedPattern(String topic) {
        Pattern pattern = this.subscribedPattern;
        if (hasPatternSubscription() && pattern != null) {
            return pattern.matcher(topic).matches();
        }
        return false;
    }

    /**
     * 获取订阅的主题
     */
    public synchronized Set<String> subscription() {
        if (hasAutoAssignedPartitions()) {
            return this.subscription;
        }
        return Collections.emptySet();
    }

    /** 获取已经暂停的topic */
    public synchronized Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused);
    }

    /**
     * 获取所有订阅的topic
     * 对于leader，这将包括所有组成员的订阅的联合。
     * 对于follower来说，它只是会员的订阅。
     * 在查询主题元数据以检测需要重新平衡的元数据更改时使用。
     * leader获取组中所有主题的元数据，以便执行分区分配(至少需要分配所有主题的分区计数)。
     *
     * Get the subscription topics for which metadata is required. For the leader, this will include
     * the union of the subscriptions of all group members. For followers, it is just that member's
     * subscription. This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     * of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    synchronized Set<String> metadataTopics() {
        if (groupSubscription.isEmpty()) {
            return subscription;
        } else if (groupSubscription.containsAll(subscription)) {
            return groupSubscription;
        } else {
            // When subscription changes `groupSubscription` may be outdated, ensure that
            // new subscription topics are returned.
            Set<String> topics = new HashSet<>(groupSubscription);
            topics.addAll(subscription);
            return topics;
        }
    }

    /**
     * 检测本consumer是否需要元数据
     * @param topic
     * @return
     */
    synchronized boolean needsMetadata(String topic) {
        return subscription.contains(topic) || groupSubscription.contains(topic);
    }

    /**
     * 获取TopicPartition对应的TopicPartitionState
     * @param tp
     * @return
     */
    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null) {
            throw new IllegalStateException("No current assignment for partition " + tp);
        }
        return state;
    }

    /**
     * 获取TopicPartition对应的TopicPartitionState
     */
    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    /**
     * 设置对应topic partition的初始消费offset
     */
    public synchronized void seekValidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekValidated(position);
    }

    /**
     * 设置对应topic partition的消费offset
     */
    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
    }

    /**
     * 设置FetchState和消费offset
     * @param tp
     * @param position
     */
    public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekUnvalidated(position);
    }

    /**
     * 设置消费状态及消费offset
     */
    synchronized void maybeSeekUnvalidated(TopicPartition tp, FetchPosition position, OffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != state.resetStrategy) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to position {}.", tp, position);
            state.seekUnvalidated(position);
        }
    }

    /**
     * 复制一份订阅的TopicPartition返回
     * @return a modifiable copy of the currently assigned partitions
     */
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(this.assignment.partitionSet());
    }

    /**
     * 复制一份订阅的TopicPartition返回
     * @return a modifiable copy of the currently assigned partitions as a list
     */
    public synchronized List<TopicPartition> assignedPartitionsList() {
        return new ArrayList<>(this.assignment.partitionSet());
    }

    /**
     * 以线程安全的方式获取订阅的主题分区数
     *
     * Provides the number of assigned partitions in a thread safe manner.
     *
     * @return the number of assigned partitions.
     */
    synchronized int numAssignedPartitions() {
        return this.assignment.size();
    }

    // Visible for testing

    /**
     * 获取可以获取数据的partition
     */
    public synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        List<TopicPartition> result = new ArrayList<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            // Cheap check is first to avoid evaluating the predicate if possible
            if (topicPartitionState.isFetchable() && isAvailable.test(topicPartition)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    /**
     * 检查是否有自动分配分区
     */
    public synchronized boolean hasAutoAssignedPartitions() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    /**
     * 设置position
     */
    public synchronized void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    /**
     * 如果已知此分区的leader支持OffsetsForLeaderEpoch API的可用版本，则输入offset验证状态。
     * 如果leader节点不支持API，只需完成偏移量验证。
     *
     * Enter the offset validation state if the leader for this partition is known to support a usable version of the
     * OffsetsForLeaderEpoch API. If the leader node does not support the API, simply complete the offset validation.
     *
     * @param apiVersions    supported API versions
     * @param tp             topic partition to validate
     * @param leaderAndEpoch leader epoch of the topic partition
     * @return true if we enter the offset validation state
     */
    public synchronized boolean maybeValidatePositionForCurrentLeader(ApiVersions apiVersions,
                                                                      TopicPartition tp,
                                                                      Metadata.LeaderAndEpoch leaderAndEpoch) {
        if (leaderAndEpoch.leader.isPresent()) {
            NodeApiVersions nodeApiVersions = apiVersions.get(leaderAndEpoch.leader.get().idString());
            if (nodeApiVersions == null || hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
            } else {
                // If the broker does not support a newer version of OffsetsForLeaderEpoch, we skip validation
                assignedState(tp).updatePositionLeaderNoValidation(leaderAndEpoch);
                return false;
            }
        } else {
            return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
        }
    }

    /**
     * 尝试使用从OffsetForLeaderEpoch请求返回的结束偏移量来完成验证。
     *
     * Attempt to complete validation with the end offset returned from the OffsetForLeaderEpoch request.
     *
     * @return  如果检测到且未定义重置策略，则记录截断详细信息。
     *          Log truncation details if detected and no reset policy is defined.
     */
    public synchronized Optional<LogTruncation> maybeCompleteValidation(TopicPartition tp,
                                                                        FetchPosition requestPosition,
                                                                        EpochEndOffset epochEndOffset) {
        // 获取partition状态
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping completed validation for partition {} which is not currently assigned.", tp);
        } else if (!state.awaitingValidation()) {
            log.debug("Skipping completed validation for partition {} which is no longer expecting validation.", tp);
        } else {
            // 获取position
            SubscriptionState.FetchPosition currentPosition = state.position;
            // 如果当前记录的position和请求的position不相同
            if (!currentPosition.equals(requestPosition)) {
                log.debug("Skipping completed validation for partition {} since the current position {} " +
                                "no longer matches the position {} when the request was sent",
                        tp, currentPosition, requestPosition);
            // 如果重置策略为空
            } else if (epochEndOffset.hasUndefinedEpochOrOffset()) {
                // 如果offset重置状态为空
                if (hasDefaultOffsetResetPolicy()) {
                    log.info("Truncation detected for partition {} at offset {}, resetting offset",
                            tp, currentPosition);
                    // 设置partition为等待重置状态
                    requestOffsetReset(tp);
                } else {
                    log.warn("Truncation detected for partition {} at offset {}, but no reset policy is set",
                            tp, currentPosition);
                    // 创建新的截断信息
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.empty()));
                }
            // 如果当前position小于请求offset
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                // 如果offset重置状态为空
                if (hasDefaultOffsetResetPolicy()) {
                    // 创建新的FetchPosition
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                            "the first offset known to diverge {}", tp, currentPosition, newPosition);
                    // 设置消费offset
                    state.seekValidated(newPosition);
                } else {
                    OffsetAndMetadata divergentOffset = new OffsetAndMetadata(epochEndOffset.endOffset(),
                            Optional.of(epochEndOffset.leaderEpoch()), null);
                    log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                                    "broker is {}), but no reset policy is set",
                            tp, currentPosition, divergentOffset);
                    // 返回新的截断信息
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.of(divergentOffset)));
                }
            } else {
                state.completeValidation();
            }
        }
        return Optional.empty();
    }

    /**
     * 检测当前分区获取状态是否为等待状态
     */
    public synchronized boolean awaitingValidation(TopicPartition tp) {
        return assignedState(tp).awaitingValidation();
    }

    /**
     * 设置当前分区为fetching(获取record)状态
     * @param tp
     */
    public synchronized void completeValidation(TopicPartition tp) {
        assignedState(tp).completeValidation();
    }

    /**
     * 获取分区对应的分区获取状态包装类
     */
    public synchronized FetchPosition validPosition(TopicPartition tp) {
        return assignedState(tp).validPosition();
    }

    /**
     * 获取分区对应的分区获取状态包装类
     */
    public synchronized FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    /**
     * 获取滞后的消息长度，会根据事务不同而不同
     * 如果是read_uncommitted：Lag = HW – ConsumerOffset
     * 如果是READ_COMMITTED：Lag = LSO – ConsumerOffset
     */
    synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        // 获取分区状态
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        } else {
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
        }
    }

    /**
     * 获取已经消费的长度
     */
    synchronized Long partitionLead(TopicPartition tp) {
        // 获取分区状态
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    /**
     * 更新HW
     */
    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark(highWatermark);
    }

    /**
     * 更新logStartOffset
     */
    synchronized void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        assignedState(tp).logStartOffset(logStartOffset);
    }

    /**
     * 更新LSO
     */
    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    /**
     * timeMs之后首选副本过期。
     * 在此之后，副本将不再有效，{@link #preferredReadReplica(TopicPartition, long)}将返回一个空结果。
     *
     * Set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result.
     *
     * @param tp                     The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs                 The time at which this preferred replica is no longer valid
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, LongSupplier timeMs) {
        // 设置超时时间
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * 获取首选副本
     * Get the preferred read replica
     *
     * @param tp     The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        // 获取分区状态
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            return topicPartitionState.preferredReadReplica(timeMs);
        }
    }

    /**
     * 清空首选副本，获取首选副本id
     *
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     *
     * @param tp The topic partition
     * @return true if the preferred read replica was set, false otherwise.
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        return assignedState(tp).clearPreferredReadReplica();
    }

    /**
     * 获取所有分区的消费状态
     */
    public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.forEach((topicPartition, partitionState) -> {
            if (partitionState.hasValidPosition()) {
                allConsumed.put(topicPartition, new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.offsetEpoch, ""));
            }
        });
        return allConsumed;
    }

    public synchronized void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        // 获取partition对应的状态，并设置为等待重置状态
        assignedState(partition).reset(offsetResetStrategy);
    }

    /**
     * 重置分区重置逻辑，并设置为等待重置状态
     */
    public synchronized void requestOffsetReset(Collection<TopicPartition> partitions, OffsetResetStrategy offsetResetStrategy) {
        partitions.forEach(tp -> {
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
            // 重置分区重置策略
            assignedState(tp).reset(offsetResetStrategy);
        });
    }

    /**
     * 设置partition为等待重置状态
     */
    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    /**
     * partitions内所有分区设置下次重试时间
     */
    synchronized void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    /**
     * 检测offset重置策略是否为空
     */
    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    /**
     * 检测分区是否为等待重置
     */
    public synchronized boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    /**
     * 分区重置测量
     * @param partition
     * @return
     */
    public synchronized OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy();
    }

    /**
     * 检测所有分区是否都有设置position
     */
    public synchronized boolean hasAllFetchPositions() {
        // Since this is in the hot-path for fetching, we do this instead of using java.util.stream API
        Iterator<TopicPartitionState> it = assignment.stateIterator();
        while (it.hasNext()) {
            // 检测是否设置position
            if (!it.next().hasValidPosition()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取所有处于初始化中的TopicPartition
     */
    public synchronized Set<TopicPartition> initializingPartitions() {
        return collectPartitions(state -> state.fetchState.equals(FetchStates.INITIALIZING));
    }

    /**
     * 获取所有分区中符合filter条件的分区
     */
    private Set<TopicPartition> collectPartitions(Predicate<TopicPartitionState> filter) {
        Set<TopicPartition> result = new HashSet<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            // 按照filter条件过滤
            if (filter.test(topicPartitionState)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    /**
     * 设置所有初始化状态的分区为等待重置状态
     * 如果有重置策略为空，则抛出异常
     */
    public synchronized void resetInitializingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        // 遍历所有分区
        assignment.forEach((tp, partitionState) -> {
            // 如果分区为初始化状态
            if (partitionState.fetchState.equals(FetchStates.INITIALIZING)) {
                // 如果重置策略为空
                if (defaultResetStrategy == OffsetResetStrategy.NONE) {
                    partitionsWithNoOffsets.add(tp);
                } else {
                    // 设置partition为等待重置状态
                    requestOffsetReset(tp);
                }
            }
        });
        // 如果有分区为初始化，并重置状态为空，抛出异常
        if (!partitionsWithNoOffsets.isEmpty()) {
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
        }
    }

    /**
     * 获取分区为重置状态，并且nowMs小于下次重试时间的所有分区
     */
    public synchronized Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        // 分区是否为重置状态，并且nowMs小于下次重试时间
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs));
    }

    /**
     * 获取分区为等待验证状态，并且nowMs小于下次重试时间的所有分区
     * @param nowMs
     * @return
     */
    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs));
    }

    /**
     * 检测分区集合中是否包含当前分区
     */
    public synchronized boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    /**
     * 检测是否包含分区并且是否处于暂停状态
     * @param tp
     * @return
     */
    public synchronized boolean isPaused(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isPaused();
    }

    /**
     * 检测分区是否设置position
     */
    synchronized boolean isFetchable(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isFetchable();
    }

    /**
     * 检测分区是否设置position
     */
    public synchronized boolean hasValidPosition(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.hasValidPosition();
    }

    /**
     * 设置分区为暂停状态
     */
    public synchronized void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    /**
     * 设置分区继续，暂停结束
     */
    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    /**
     * 给定分区集合设置下次重试时间
     * @param partitions
     * @param nextRetryTimeMs
     */
    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions) {
            // by the time the request failed, the assignment may no longer
            // contain this partition any more, in which case we would just ignore.
            final TopicPartitionState state = assignedStateOrNull(partition);
            if (state != null) {
                state.requestFailed(nextRetryTimeMs);
            }
        }
    }

    /**
     * 把分区移动到分区链表的队尾
     */
    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    /**
     * 获取ConsumerRebalanceListener
     */
    public synchronized ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    /**
     * topic-partition状态维护包装类
     */
    private static class TopicPartitionState {
        /** 当前分区的获取状态 */
        private FetchState fetchState;
        /** 记录了下次要从kafka服务端获取的消息的offset */
        private FetchPosition position; // last consumed position
        /** HW */
        private Long highWatermark; // the high watermark from last fetch
        /** log开始offset */
        private Long logStartOffset; // the log start offset
        /** LSO之前的消息状态都已确认（commit/aborted）主要用于事务 */
        private Long lastStableOffset;
        /** 记录当前TopicPartition是否处于暂停状态 */
        private boolean paused;  // whether this partition has been paused by the user
        /** OffsetResetStrategy的枚举类型，重置position的策略。同时，此字段是否为空，也表示了是否需要重置position的值 */
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        /** 下次重试时间 */
        private Long nextRetryTimeMs;
        /** 优先副本 */
        private Integer preferredReadReplica;
        /** 首选副本过期时间 */
        private Long preferredReadReplicaExpireTimeMs;

        TopicPartitionState() {
            this.paused = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        /**
         * 转换成新的状态，并启动线程
         * @param newState
         * @param runIfTransitioned
         */
        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            // 旧的state可转换的state中是否包含newState
            FetchState nextState = this.fetchState.transitionTo(newState);
            // 如果包含的话，就直接转换
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                // 启动线程进行设置
                runIfTransitioned.run();
                if (this.position == null && nextState.requiresPosition()) {
                    throw new IllegalStateException("Transitioned subscription state to " + nextState + ", but position is null");
                } else if (!nextState.requiresPosition()) {
                    this.position = null;
                }
            }
        }

        /**
         * 返回首选副本
         * @param timeMs
         * @return
         */
        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        /**
         * 设置优先副本
         */
        private void updatePreferredReadReplica(int preferredReadReplica, LongSupplier timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.getAsLong();
            }
        }

        /**
         * 清空首选副本，并返回首选副本id
         * @return
         */
        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        /**
         * 设置为等待重置状态
         */
        private void reset(OffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * 检查分区是否存leader，并且分区获取状态为等待验证状态
         * 该方法还将更新leader
         *
         * Check if the position exists and needs to be validated. If so, enter the AWAIT_VALIDATION state. This method
         * also will update the position with the current leader and epoch.
         *
         * @param currentLeaderAndEpoch leader and epoch to compare the offset with
         * @return true if the position is now awaiting validation
         */
        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            // 检测分区状态是否为等待重置
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }
            // 是否包含leader
            if (!currentLeaderAndEpoch.leader.isPresent()) {
                return false;
            }

            // 如果和分区leader和传入的不一样
            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                // 重置leader
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                // 设置FetchStates及初始位置
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            // 查看分区的获取状态是否为等待验证状态
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * 对于旧版本的API，我们不能执行偏移验证，因此直接转换到fetching(获取record)状态
         *
         * For older versions of the API, we cannot perform offset validation so we simply transition directly to FETCHING
         */
        private void updatePositionLeaderNoValidation(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            // 如果没有设置下次需要从broker获取消息的offset
            if (position != null) {
                // 转换成fetching状态
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * 设置FetchStates及初始位置
         */
        private void validatePosition(FetchPosition position) {
            // 如果offsetEpoch和currentLeader.epoch包含值
            if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {
                // 转换为等待验证状态以及设置position
                transitionState(FetchStates.AWAIT_VALIDATION, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            } else {
                // If we have no epoch information for the current position, then we can skip validation
                // 设置position
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * 清除等待验证状态并进入获取状态。
         *
         * Clear the awaiting validation state and enter fetching.
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> this.nextRetryTimeMs = null);
            }
        }

        /**
         * 检测当前分区的获取状态是否为等待状态
         */
        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * 检测当前时间是小于下次重试时间
         */
        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        /**
         * 分区是否为等待重置
         */
        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        /**
         * 设置下次重试时间
         * @param nextAllowedRetryTimeMs
         */
        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        /**
         * 设置下次重试时间
         * @param nextAllowedRetryTimeMs
         */
        private void requestFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        /**
         * 检测是否设置position
         */
        private boolean hasValidPosition() {
            return fetchState.hasValidPosition();
        }

        /**
         * 检测是否设置下次从broker获取消息的offset
         * @return
         */
        private boolean hasPosition() {
            return position != null;
        }

        private boolean isPaused() {
            return paused;
        }

        /**
         * 设置下次消费offset为给定position
         */
        private void seekValidated(FetchPosition position) {
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * 设置状态及消费offset
         */
        private void seekUnvalidated(FetchPosition fetchPosition) {
            // 设置消费offset
            seekValidated(fetchPosition);
            // 设置状态及初始offset
            validatePosition(fetchPosition);
        }

        /**
         * 设置position
         * @param position
         */
        private void position(FetchPosition position) {
            if (!hasValidPosition()) {
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            }
            this.position = position;
        }

        /**
         * 如果有设置position，则返回，否则返回null
         * @return
         */
        private FetchPosition validPosition() {
            // 如果有设置position则返回
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        /**
         * 暂停分区
         */
        private void pause() {
            this.paused = true;
        }

        /**
         * 重启分区
         */
        private void resume() {
            this.paused = false;
        }

        /**
         * 检测分区未暂停，并设置了position
         * @return
         */
        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

        /**
         * 设置HW
         */
        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
        }

        /**
         * 设置logStartOffset
         * @param logStartOffset
         */
        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        /**
         * 设置LSO
         */
        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
        }

        /**
         * 获取重置position的策略
         * @return
         */
        private OffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    /**
     * 分区的获取状态。
     * 这个类用于确定有效的状态转换并公开当前获取状态的一些行为。实际的状态变量存储在{@link TopicPartitionState}中
     * <p>
     * The fetch state of a partition. This class is used to determine valid state transitions and expose the some of
     * the behavior of the current fetch state. Actual state variables are stored in the {@link TopicPartitionState}.
     */
    interface FetchState {

        /**
         * 旧的state可转换的state中是否包含newState
         * @param newState
         * @return
         */
        default FetchState transitionTo(FetchState newState) {
            if (validTransitions().contains(newState)) {
                return newState;
            } else {
                return this;
            }
        }

        /**
         * 返回此状态可以转换到的有效状态
         * <p>
         * Return the valid states which this state can transition to
         */
        Collection<FetchState> validTransitions();

        /**
         * 测试此状态是否需要设置位置
         * <p>
         * Test if this state requires a position to be set
         */
        boolean requiresPosition();

        /**
         * 测试此状态是否被认为具有可用于抓取的有效位置
         * <p>
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        boolean hasValidPosition();
    }

    /**
     * 所有可能获取状态的枚举。状态转换被编码在{@link FetchState#validTransitions}返回的值中
     *
     * An enumeration of all the possible fetch states. The state transitions are encoded in the values returned by
     * {@link FetchState#validTransitions}.
     */
    enum FetchStates implements FetchState {
        /**
         * 初始化状态
         */
        INITIALIZING() {
            /**
             * 可转换的状态
             * @return
             */
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },
        /**
         * 处于正常消费中
         */
        FETCHING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return true;
            }
        },
        /**
         * 等待重置
         */
        AWAIT_RESET() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },
        /**
         * 等待验证
         */
        AWAIT_VALIDATION() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        }
    }

    /**
     * 表示分区订阅的位置。
     *
     * Represents the position of a partition subscription.
     * <p>
     * This includes the offset and epoch from the last record in
     * the batch from a FetchResponse. It also includes the leader epoch at the time the batch was consumed.
     */
    public static class FetchPosition {
        /** 最后一条消息的offset */
        public final long offset;
        /** 最后一条消息的版本 */
        final Optional<Integer> offsetEpoch;
        /** leader信息 */
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }

    /**
     * 日志截断
     */
    public static class LogTruncation {
        public final TopicPartition topicPartition;
        public final FetchPosition fetchPosition;
        public final Optional<OffsetAndMetadata> divergentOffsetOpt;

        public LogTruncation(TopicPartition topicPartition,
                             FetchPosition fetchPosition,
                             Optional<OffsetAndMetadata> divergentOffsetOpt) {
            this.topicPartition = topicPartition;
            this.fetchPosition = fetchPosition;
            this.divergentOffsetOpt = divergentOffsetOpt;
        }

        @Override
        public String toString() {
            StringBuilder bldr = new StringBuilder()
                    .append("(partition=")
                    .append(topicPartition)
                    .append(", fetchOffset=")
                    .append(fetchPosition.offset)
                    .append(", fetchEpoch=")
                    .append(fetchPosition.offsetEpoch);

            if (divergentOffsetOpt.isPresent()) {
                OffsetAndMetadata divergentOffset = divergentOffsetOpt.get();
                bldr.append(", divergentOffset=")
                        .append(divergentOffset.offset())
                        .append(", divergentEpoch=")
                        .append(divergentOffset.leaderEpoch());
            } else {
                bldr.append(", divergentOffset=unknown")
                        .append(", divergentEpoch=unknown");
            }

            return bldr.append(")").toString();

        }
    }
}