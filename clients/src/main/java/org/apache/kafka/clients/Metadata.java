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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * 集群元数据，主要用于记录
 * 围绕元数据封装一些逻辑的类。
 * 这个类由客户端线程(用于分区)和后台发送方线程共享。
 * 元数据仅为主题的子集维护，可以随时间添加。当我们请求一个主题的元数据时，我们没有它的元数据，这将触发元数据更新。
 *
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public class Metadata implements Closeable {
    private final Logger log;
    /** 两次发出更新cluster保存的元数据信息的最小时间差，默认为10ms。这是为了防止更新操作过于频繁而造成网络阻塞和增加服务端压力。 */
    private final long refreshBackoffMs;
    /** 每隔多久更新一次，默认300*1000(5min) */
    private final long metadataExpireMs;
    private int updateVersion;  // bumped on every metadata response
    private int requestVersion; // bumped on every new topic addition
    /** 记录上一次更新元数据的时间戳（也包含更新失败的情况） */
    private long lastRefreshMs;
    /** 上一次成功更新的时间戳。如果每次都成功，则 lastSuccessfulRefreshMs、lastRefreshMs相等，否则lastRefreshMs>lastSuccessfulRefreshMs */
    private long lastSuccessfulRefreshMs;
    private KafkaException fatalException;
    /** 无效主题 */
    private Set<String> invalidTopics;
    /** 未授权主题 */
    private Set<String> unauthorizedTopics;
    /** 集群元数据缓存 */
    private MetadataCache cache = MetadataCache.empty();
    /** 整个元数据都需要更新 */
    private boolean needFullUpdate;
    /** partition元数据需要更新 */
    private boolean needPartialUpdate;
    /** 集群更新监听器 */
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean isClosed;
    /** partition leader版本号 */
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;

    /**
     * 创建一个新的元数据实例，所有都为空。
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param logContext               Log context corresponding to the containing client
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.log = logContext.logger(Metadata.class);
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needFullUpdate = false;
        this.needPartialUpdate = false;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
        this.invalidTopics = Collections.emptySet();
        this.unauthorizedTopics = Collections.emptySet();
    }

    /**
     * 直接返回Cluster信息
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return cache.cluster();
    }

    /**
     * 返回下一次更新的时间
     *
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * 返回下一次更新剩余的时间
     *
     * 下一次更新集群信息的时间是当前信息过期的最长时间和当前信息可以更新的时间(即后退时间已经过去);
     * 如果已请求更新，则到期时间为现在
     *
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = updateRequested() ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    /**
     * 获取过期时间
     */
    public long metadataExpireMs() {
        return this.metadataExpireMs;
    }

    /**
     * 设置元数据需要更新
     * Request an update of the current cluster metadata info, return the current updateVersion before the update
     */
    public synchronized int requestUpdate() {
        this.needFullUpdate = true;
        return this.updateVersion;
    }

    /**
     * 设置需要更新partition元数据
     * 当有新的topic时，
     */
    public synchronized int requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        this.needPartialUpdate = true;
        this.requestVersion++;
        return this.updateVersion;
    }

    /**
     * 更新partition leader版本号，并设置是否需要更新
     * Request an update for the partition metadata iff we have seen a newer leader epoch. This is called by the client
     * any time it handles a response from the broker that includes leader epoch, except for UpdateMetadata which
     * follows a different code path ({@link #update}).
     *
     * @param topicPartition
     * @param leaderEpoch
     * @return true if we updated the last seen epoch, false otherwise
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        // 版本验证
        if (leaderEpoch < 0) {
            throw new IllegalArgumentException("Invalid leader epoch " + leaderEpoch + " (must be non-negative)");
        }
        // 获取旧的版本号
        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        log.trace("Determining if we should replace existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);

        final boolean updated;
        if (oldEpoch == null) {
            log.debug("Not replacing null epoch with new epoch {} for partition {}", leaderEpoch, topicPartition);
            updated = false;
        } else if (leaderEpoch > oldEpoch) {
            log.debug("Updating last seen epoch from {} to {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, leaderEpoch);
            updated = true;
        } else {
            log.debug("Not replacing existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            updated = false;
        }
        // 设置是否需要更新
        this.needFullUpdate = this.needFullUpdate || updated;
        return updated;
    }

    /**
     * 获取partition版本号
     * @param topicPartition
     * @return
     */
    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * 检查是否需要更新
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needFullUpdate || this.needPartialUpdate;
    }

    /**
     * 获取partition元数据
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    synchronized Optional<MetadataResponse.PartitionMetadata> partitionMetadataIfCurrent(TopicPartition topicPartition) {
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        Optional<MetadataResponse.PartitionMetadata> partitionMetadata = cache.partitionMetadata(topicPartition);
        if (epoch == null) {
            // old cluster format (no epochs)
            return partitionMetadata;
        } else {
            return partitionMetadata.filter(metadata ->
                    metadata.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH).equals(epoch));
        }
    }

    /**
     * 获取当前partition的LeaderAndEpoch包装类
     */
    public synchronized LeaderAndEpoch currentLeader(TopicPartition topicPartition) {
        // 获取partition元数据
        Optional<MetadataResponse.PartitionMetadata> maybeMetadata = partitionMetadataIfCurrent(topicPartition);
        // 没有数据的话，就构造一个空的
        if (!maybeMetadata.isPresent()) {
            return new LeaderAndEpoch(Optional.empty(), Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition)));
        }

        MetadataResponse.PartitionMetadata partitionMetadata = maybeMetadata.get();
        Optional<Integer> leaderEpochOpt = partitionMetadata.leaderEpoch;
        Optional<Node> leaderNodeOpt = partitionMetadata.leaderId.flatMap(cache::nodeById);
        // 构造LeaderAndEpoch对象返回
        return new LeaderAndEpoch(leaderNodeOpt, leaderEpochOpt);
    }

    /**
     * 初始化元数据信息
     */
    public synchronized void bootstrap(List<InetSocketAddress> addresses) {
        this.needFullUpdate = true;
        this.updateVersion += 1;
        // 根据address构建节点元数据缓存
        this.cache = MetadataCache.bootstrap(addresses);
    }

    /**
     * Update metadata assuming the current request version.
     *
     * For testing only.
     */
    public synchronized void updateWithCurrentRequestVersion(MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        this.update(this.requestVersion, response, isPartialUpdate, nowMs);
    }

    /**
     * 更新集群元数据
     * 如果启用了topic过期，则为必要的topic设置过期时间，过期的topic将从元数据中删除。
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion(long)}.
     * @param response metadata response received from the broker
     * @param isPartialUpdate whether the metadata request was for a subset of the active topics
     * @param nowMs current time in milliseconds
     */
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed()) {
            throw new IllegalStateException("Update requested after metadata close");
        }

        this.needPartialUpdate = requestVersion < this.requestVersion;
        this.lastRefreshMs = nowMs;
        this.updateVersion += 1;
        if (!isPartialUpdate) {
            this.needFullUpdate = false;
            this.lastSuccessfulRefreshMs = nowMs;
        }
        // 获取上个集群id
        String previousClusterId = cache.clusterResource().clusterId();

        this.cache = handleMetadataResponse(response, isPartialUpdate, nowMs);

        Cluster cluster = cache.cluster();
        maybeSetMetadataError(cluster);

        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, nowMs));

        String newClusterId = cache.clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {
            log.info("Cluster ID: {}", newClusterId);
        }
        clusterResourceListeners.onUpdate(cache.clusterResource());

        log.debug("Updated cluster metadata updateVersion {} to {}", this.updateVersion, this.cache);
    }

    private void maybeSetMetadataError(Cluster cluster) {
        clearRecoverableErrors();
        checkInvalidTopics(cluster);
        checkUnauthorizedTopics(cluster);
    }

    private void checkInvalidTopics(Cluster cluster) {
        if (!cluster.invalidTopics().isEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics());
            invalidTopics = new HashSet<>(cluster.invalidTopics());
        }
    }

    private void checkUnauthorizedTopics(Cluster cluster) {
        if (!cluster.unauthorizedTopics().isEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics());
            unauthorizedTopics = new HashSet<>(cluster.unauthorizedTopics());
        }
    }

    /**
     * 将MetadataResponse转换为MetadataCache
     * Transform a MetadataResponse into a new MetadataCache instance.
     */
    private MetadataCache handleMetadataResponse(MetadataResponse metadataResponse, boolean isPartialUpdate, long nowMs) {
        // All encountered topics.
        Set<String> topics = new HashSet<>();

        // Retained topics to be passed to the metadata cache.
        // 需要保留的topic等信息
        Set<String> internalTopics = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();
        Set<String> invalidTopics = new HashSet<>();

        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
        // 遍历topic信息
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            // 先加入topics中
            topics.add(metadata.topic());
            // 检查是否包含topic
            if (!retainTopic(metadata.topic(), metadata.isInternal(), nowMs)) {
                continue;
            }
            // 如果是内部主题，则直接加入
            if (metadata.isInternal()) {
                internalTopics.add(metadata.topic());
            }
            // 如果没有错误信息
            if (metadata.error() == Errors.NONE) {
                // 遍历partition信息
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    // Even if the partition's metadata includes an error, we need to handle
                    // the update to catch new epochs
                    // 更新partition元数据信息
                    updateLatestMetadata(partitionMetadata, metadataResponse.hasReliableLeaderEpochs())
                        .ifPresent(partitions::add);

                    if (partitionMetadata.error.exception() instanceof InvalidMetadataException) {
                        log.debug("Requesting metadata update for partition {} due to error {}",
                                partitionMetadata.topicPartition, partitionMetadata.error);
                        // 如果partition有错误信息，则设置需要更新
                        requestUpdate();
                    }
                }
            } else {
                if (metadata.error().exception() instanceof InvalidMetadataException) {
                    log.debug("Requesting metadata update for topic {} due to error {}", metadata.topic(), metadata.error());
                    // 设置需要更新
                    requestUpdate();
                }
                // 加入到无效主题集合中
                if (metadata.error() == Errors.INVALID_TOPIC_EXCEPTION) {
                    invalidTopics.add(metadata.topic());
                // 加入到验证失败主题集合中
                } else if (metadata.error() == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(metadata.topic());
                }
            }
        }

        // 获取节点信息
        Map<Integer, Node> nodes = metadataResponse.brokersById();
        // 如果是部分更新，则合并后返回
        if (isPartialUpdate) {
            return this.cache.mergeWith(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(),
                (topic, isInternal) -> !topics.contains(topic) && retainTopic(topic, isInternal, nowMs));
        // 创建新的元数据缓存返回
        } else {
            return new MetadataCache(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller());
        }
    }

    /**
     * 根据leader版本信息，更新后的partition信息
     * Compute the latest partition metadata to cache given ordering by leader epochs (if both
     * available and reliable).
     */
    private Optional<MetadataResponse.PartitionMetadata> updateLatestMetadata(
            MetadataResponse.PartitionMetadata partitionMetadata,
            boolean hasReliableLeaderEpoch) {
        TopicPartition tp = partitionMetadata.topicPartition;
        if (hasReliableLeaderEpoch && partitionMetadata.leaderEpoch.isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch.get();
            // If the received leader epoch is at least the same as the previous one, update the metadata
            Integer currentEpoch = lastSeenLeaderEpochs.get(tp);
            if (currentEpoch == null || newEpoch >= currentEpoch) {
                log.debug("Updating last seen epoch for partition {} from {} to epoch {} from new metadata", tp, currentEpoch, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                return Optional.of(partitionMetadata);
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                log.debug("Got metadata for an older epoch {} (current is {}) for partition {}, not updating", newEpoch, currentEpoch, tp);
                return cache.partitionMetadata(tp);
            }
        } else {
            // Handle old cluster formats as well as error responses where leader and epoch are missing
            lastSeenLeaderEpochs.remove(tp);
            return Optional.of(partitionMetadata.withoutLeaderEpoch());
        }
    }

    /**
     * 如果在元数据更新期间遇到任何不可检索的异常，清除并抛出异常。
     *
     * If any non-retriable exceptions were encountered during metadata update, clear and throw the exception.
     * This is used by the consumer to propagate any fatal exceptions or topic exceptions for any of the topics
     * in the consumer's Metadata.
     */
    public synchronized void maybeThrowAnyException() {
        clearErrorsAndMaybeThrowException(this::recoverableException);
    }

    /**
     * 如果在元数据更新期间遇到任何致命异常，则抛出异常。
     *
     * If any fatal exceptions were encountered during metadata update, throw the exception. This is used by
     * the producer to abort waiting for metadata if there were fatal exceptions (e.g. authentication failures)
     * in the last metadata update.
     */
    protected synchronized void maybeThrowFatalException() {
        KafkaException metadataException = this.fatalException;
        if (metadataException != null) {
            fatalException = null;
            throw metadataException;
        }
    }

    /**
     * 如果在元数据更新期间遇到任何不可检索的异常，如果异常是致命的或与指定主题相关，则抛出异常。清除上次元数据更新的所有异常。
     * 生产者使用它传播发送请求的主题元数据错误。
     *
     * If any non-retriable exceptions were encountered during metadata update, throw exception if the exception
     * is fatal or related to the specified topic. All exceptions from the last metadata update are cleared.
     * This is used by the producer to propagate topic metadata errors for send requests.
     */
    public synchronized void maybeThrowExceptionForTopic(String topic) {
        clearErrorsAndMaybeThrowException(() -> recoverableExceptionForTopic(topic));
    }

    /**
     * 清除并抛出异常
     */
    private void clearErrorsAndMaybeThrowException(Supplier<KafkaException> recoverableExceptionSupplier) {
        KafkaException metadataException = Optional.ofNullable(fatalException).orElseGet(recoverableExceptionSupplier);
        fatalException = null;
        clearRecoverableErrors();
        if (metadataException != null) {
            throw metadataException;
        }
    }

    // We may be able to recover from this exception if metadata for this topic is no longer needed
    /**
     * 检测topic覆盖过程中是否有异常发生
     */
    private KafkaException recoverableException() {
        if (!unauthorizedTopics.isEmpty()) {
            return new TopicAuthorizationException(unauthorizedTopics);
        } else if (!invalidTopics.isEmpty()) {
            return new InvalidTopicException(invalidTopics);
        } else {
            return null;
        }
    }

    /**
     * 检测topic更新中是否发生异常
     * @param topic
     * @return
     */
    private KafkaException recoverableExceptionForTopic(String topic) {
        if (unauthorizedTopics.contains(topic)) {
            return new TopicAuthorizationException(Collections.singleton(topic));
        } else if (invalidTopics.contains(topic)) {
            return new InvalidTopicException(Collections.singleton(topic));
        } else {
            return null;
        }
    }

    /**
     * 清空异常topic
     */
    private void clearRecoverableErrors() {
        invalidTopics = Collections.emptySet();
        unauthorizedTopics = Collections.emptySet();
    }

    /**
     * 元数据更新失败，记录最后更新时间。防止立即重试
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * 设置异常
     * Propagate a fatal error which affects the ability to fetch metadata for the cluster.
     * Two examples are authentication and unsupported version exceptions.
     *
     * @param exception The fatal exception
     */
    public synchronized void fatalError(KafkaException exception) {
        this.fatalException = exception;
    }

    /**
     * 获取更新的版本号
     * @return The current metadata updateVersion
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * 关闭此元数据实例，表示不再可能进行元数据更新。
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * 检查是否关闭
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    /**
     * 获取MetadataRequest构建器
     * @param nowMs
     * @return
     */
    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion(long nowMs) {
        MetadataRequest.Builder request = null;
        boolean isPartialUpdate = false;

        // Perform a partial update only if a full update hasn't been requested, and the last successful
        // hasn't exceeded the metadata refresh time.
        // 检测是否部分更新
        if (!this.needFullUpdate && this.lastSuccessfulRefreshMs + this.metadataExpireMs > nowMs) {
            request = newMetadataRequestBuilderForNewTopics();
            isPartialUpdate = true;
        }

        if (request == null) {
            // 获取构建器
            request = newMetadataRequestBuilder();
            isPartialUpdate = false;
        }
        return new MetadataRequestAndVersion(request, requestVersion, isPartialUpdate);
    }

    /**
     * 构造并返回元数据请求生成器，用于获取集群数据和所有活动topic。
     *
     * Constructs and returns a metadata request builder for fetching cluster data and all active topics.
     *
     * @return the constructed non-null metadata builder
     */
    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    /**
     * 构造并返回一个元数据请求生成器，用于获取集群数据和任何未激活的主题
     * 如果不支持该功能，则为空。
     *
     * Constructs and returns a metadata request builder for fetching cluster data and any uncached topics,
     * otherwise null if the functionality is not supported.
     *
     * @return the constructed metadata builder, or null if not supported
     */
    protected MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return null;
    }

    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    /**
     * MetadataRequest构建器
     */
    public static class MetadataRequestAndVersion {
        public final MetadataRequest.Builder requestBuilder;
        public final int requestVersion;
        public final boolean isPartialUpdate;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion,
                                          boolean isPartialUpdate) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
            this.isPartialUpdate = isPartialUpdate;
        }
    }

    /**
     * 表示元数据中已知的当前leader状态。
     * 如果元数据是从不支持足够的元数据API版本的代理接收的，那么我们可能知道leader，但不知道epoch。
     * 也有可能我们知道leader epoch，但不知道来自外部来源的leader epoch(例如，一个已经commit的偏移量)。
     *
     * Represents current leader state known in metadata. It is possible that we know the leader, but not the
     * epoch if the metadata is received from a broker which does not support a sufficient Metadata API version.
     * It is also possible that we know of the leader epoch, but not the leader when it is derived
     * from an external source (e.g. a committed offset).
     */
    public static class LeaderAndEpoch {
        private static final LeaderAndEpoch NO_LEADER_OR_EPOCH = new LeaderAndEpoch(Optional.empty(), Optional.empty());
        /** leader节点 */
        public final Optional<Node> leader;
        /** leader版本号 */
        public final Optional<Integer> epoch;

        public LeaderAndEpoch(Optional<Node> leader, Optional<Integer> epoch) {
            this.leader = Objects.requireNonNull(leader);
            this.epoch = Objects.requireNonNull(epoch);
        }

        public static LeaderAndEpoch noLeaderOrEpoch() {
            return NO_LEADER_OR_EPOCH;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LeaderAndEpoch that = (LeaderAndEpoch) o;

            if (!leader.equals(that.leader)) {
                return false;
            }
            return epoch.equals(that.epoch);
        }

        @Override
        public int hashCode() {
            int result = leader.hashCode();
            result = 31 * result + epoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                    '}';
        }
    }
}
