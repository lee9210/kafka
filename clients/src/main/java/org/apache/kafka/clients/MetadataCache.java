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
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Kafka集群中节点、主题和分区的内部可变缓存。
 * 这将保持一个最新的集群实例，该实例为读访问进行了优化。
 *
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
public class MetadataCache {
    private final String clusterId;
    /** 节点 */
    private final Map<Integer, Node> nodes;
    /** 未授权的主题 */
    private final Set<String> unauthorizedTopics;
    /** 无效的主题 */
    private final Set<String> invalidTopics;
    /** 内部主题 */
    private final Set<String> internalTopics;
    /** controller所在node */
    private final Node controller;
    /** partition元数据 */
    private final Map<TopicPartition, PartitionMetadata> metadataByPartition;

    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  Map<Integer, Node> nodes,
                  Collection<PartitionMetadata> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    private MetadataCache(String clusterId,
                          Map<Integer, Node> nodes,
                          Collection<PartitionMetadata> partitions,
                          Set<String> unauthorizedTopics,
                          Set<String> invalidTopics,
                          Set<String> internalTopics,
                          Node controller,
                          Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.unauthorizedTopics = unauthorizedTopics;
        this.invalidTopics = invalidTopics;
        this.internalTopics = internalTopics;
        this.controller = controller;

        this.metadataByPartition = new HashMap<>(partitions.size());
        for (PartitionMetadata p : partitions) {
            this.metadataByPartition.put(p.topicPartition, p);
        }

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    /**
     * 获取partition元数据缓存
     */
    Optional<PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition));
    }

    /**
     * 获取node
     */
    Optional<Node> nodeById(int id) {
        return Optional.ofNullable(nodes.get(id));
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    ClusterResource clusterResource() {
        return new ClusterResource(clusterId);
    }

    /**
     * 将元数据缓存的内容与所提供的元数据合并，返回新的元数据缓存。
     * 所提供的元数据被假定比缓存的元数据更近，因此所有重复的元数据都将被覆盖。
     *
     * Merges the metadata cache's contents with the provided metadata, returning a new metadata cache. The provided
     * metadata is presumed to be more recent than the cache's metadata, and therefore all overlapping metadata will
     * be overridden.
     *
     * @param newClusterId the new cluster Id
     * @param newNodes the new set of nodes
     * @param addPartitions partitions to add
     * @param addUnauthorizedTopics unauthorized topics to add
     * @param addInternalTopics internal topics to add
     * @param newController the new controller node
     * @param retainTopic returns whether a topic's metadata should be retained
     * @return the merged metadata cache
     */
    MetadataCache mergeWith(String newClusterId,
                            Map<Integer, Node> newNodes,
                            Collection<PartitionMetadata> addPartitions,
                            Set<String> addUnauthorizedTopics,
                            Set<String> addInvalidTopics,
                            Set<String> addInternalTopics,
                            Node newController,
                            BiPredicate<String, Boolean> retainTopic) {
        // 应该保留的主题
        Predicate<String> shouldRetainTopic = topic -> retainTopic.test(topic, internalTopics.contains(topic));
        // 新的partition元数据
        Map<TopicPartition, PartitionMetadata> newMetadataByPartition = new HashMap<>(addPartitions.size());
        for (PartitionMetadata partition : addPartitions) {
            newMetadataByPartition.put(partition.topicPartition, partition);
        }
        // 覆盖
        for (Map.Entry<TopicPartition, PartitionMetadata> entry : metadataByPartition.entrySet()) {
            if (shouldRetainTopic.test(entry.getKey().topic())) {
                newMetadataByPartition.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        // 覆盖
        Set<String> newUnauthorizedTopics = fillSet(addUnauthorizedTopics, unauthorizedTopics, shouldRetainTopic);
        Set<String> newInvalidTopics = fillSet(addInvalidTopics, invalidTopics, shouldRetainTopic);
        Set<String> newInternalTopics = fillSet(addInternalTopics, internalTopics, shouldRetainTopic);
        // 构造新的元数据缓存
        return new MetadataCache(newClusterId, newNodes, newMetadataByPartition.values(), newUnauthorizedTopics,
                newInvalidTopics, newInternalTopics, newController);
    }

    /**
     * 复制{@code baseSet}并在{@code fillSet}中添加所有不存在的元素，使{@code predicate}为真。
     * 换句话说，{@code baseSet}的所有元素都将包含在结果中，在predicate为true的情况下，{@code fillSet}中将包含额外的非重叠元素。
     *
     * Copies {@code baseSet} and adds all non-existent elements in {@code fillSet} such that {@code predicate} is true.
     * In other words, all elements of {@code baseSet} will be contained in the result, with additional non-overlapping
     * elements in {@code fillSet} where the predicate is true.
     *
     * @param baseSet the base elements for the resulting set
     * @param fillSet elements to be filled into the resulting set
     * @param predicate tested against the fill set to determine whether elements should be added to the base set
     */
    private static <T> Set<T> fillSet(Set<T> baseSet, Set<T> fillSet, Predicate<T> predicate) {
        Set<T> result = new HashSet<>(baseSet);
        for (T element : fillSet) {
            if (predicate.test(element)) {
                result.add(element);
            }
        }
        return result;
    }

    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata, nodes))
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes.values(), partitionInfos, unauthorizedTopics,
                invalidTopics, internalTopics, controller);
    }

    /**
     * 根据address构建节点元数据缓存
     */
    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        Map<Integer, Node> nodes = new HashMap<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses) {
            nodes.put(nodeId, new Node(nodeId, address.getHostString(), address.getPort()));
            nodeId--;
        }
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                null, Cluster.bootstrap(addresses));
    }

    /**
     * 创建空的节点元数据将缓存
     * @return
     */
    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyMap(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values() +
                ", controller=" + controller +
                '}';
    }

}
