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
package org.apache.kafka.clients.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;

/**
 *
 * 主要是为了在无key的情况下，通过随机发送到不同的partition中，构成的batch会更平衡
 * 分区策略是：
 * 1. 如果在record中指定了分区，则使用此分区
 * 2. 否则使用sticky partition指定,主要是增加到上次发送的batch中
 * The partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>Otherwise choose the sticky partition that changes when the batch is full.
 * 
 * NOTE: In constrast to the DefaultPartitioner, the record key is NOT used as part of the partitioning strategy in this 
 *       partitioner. Records with the same key are not guaranteed to be sent to the same partition.
 * 
 * See KIP-480 for details about sticky partitioning.
 */
public class UniformStickyPartitioner implements Partitioner {

    /** 分区发送缓存 */
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    @Override
    public void configure(Map<String, ?> configs) {}

    /**
     * 获取分区
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return stickyPartitionCache.partition(topic, cluster);
    }

    @Override
    public void close() {}
    
    /**
     * 如果当前粘分区的batch完成，则更改粘分区。
     * 或者，如果没有确定粘分区，则随机设置一个。
     *
     * If a batch completed for the current sticky partition, change the sticky partition.
     * Alternately, if no sticky partition has been determined, set one.
     */
    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        // 设置新的分区
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
