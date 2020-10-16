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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

/**
 * 缓存跟踪任何给定主题的当前分区。这个类不应该在外部使用。
 * 主要是为了在无key的情况下，通过粘粘到上次发送的分区中
 *
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks the current sticky
 * partition for any given topic. This class should not be used externally. 
 */
public class StickyPartitionCache {
    /** 存储topic和partition的对应关系 */
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    /**
     * 获取topic的对应的发送分区
     */
    public int partition(String topic, Cluster cluster) {
        // 获取topic对应的分区
        Integer part = indexCache.get(topic);
        if (part == null) {
            // 获取新的
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    /**
     * 获取新的partition位置
     * @return
     */
    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        Integer oldPart = indexCache.get(topic);
        Integer newPart = oldPart;
        // Check that the current sticky partition for the topic is either not set or that the partition that 
        // triggered the new batch matches the sticky partition that needs to be changed.
        // 检查主题的当前粘分区是否未设置，或者触发新批处理的分区是否与需要更改的粘分区匹配。
        // 如果上次发送的分区和缓存的分区相同
        if (oldPart == null || oldPart == prevPartition) {
            // 获取存活的partition
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            // 如果存活数小于1
            if (availablePartitions.size() < 1) {
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();
            // 如果存活数等于1
            } else if (availablePartitions.size() == 1) {
                newPart = availablePartitions.get(0).partition();
            // 如果大于1
            } else {
                // 选择一个和上次发送不同的partition
                while (newPart == null || newPart.equals(oldPart)) {
                    Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            // 只有在粘贴分区为null或prevPartition与当前粘贴分区匹配时才更改它。
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        return indexCache.get(topic);
    }

}