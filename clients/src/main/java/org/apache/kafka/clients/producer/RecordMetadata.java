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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;

/**
 * 已被服务器确认的记录的元数据
 * The metadata for a record that has been acknowledged by the server
 */
public final class RecordMetadata {

    /**
     * Partition value for record without partition assigned
     */
    public static final int UNKNOWN_PARTITION = -1;
    /** 本次发送数据的最后一条消息的偏移量 */
    private final long offset;
    // The timestamp of the message.
    // If LogAppendTime is used for the topic, the timestamp will be the timestamp returned by the broker.
    // If CreateTime is used for the topic, the timestamp is the timestamp in the corresponding ProducerRecord if the
    // user provided one. Otherwise, it will be the producer local time when the producer record was handed to the
    // producer.
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final TopicPartition topicPartition;
    /** 验证码 */
    private volatile Long checksum;

    /**
     * 创建一个实例
     */
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset, long timestamp,
                          Long checksum, int serializedKeySize, int serializedValueSize) {
        // ignore the relativeOffset if the base offset is -1,
        // since this indicates the offset is unknown
        this.offset = baseOffset == -1 ? baseOffset : baseOffset + relativeOffset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    /**
     * 检查元数据是否包含offset
     * Indicates whether the record metadata includes the offset.
     * @return true if the offset is included in the metadata, false otherwise.
     */
    public boolean hasOffset() {
        return this.offset != ProduceResponse.INVALID_OFFSET;
    }

    /**
     * 返回offset
     * The offset of the record in the topic/partition.
     * @return the offset of the record, or -1 if {{@link #hasOffset()}} returns false.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * Indicates whether the record metadata includes the timestamp.
     * @return true if a valid timestamp exists, false otherwise.
     */
    public boolean hasTimestamp() {
        return this.timestamp != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * 返回创建时间
     * The timestamp of the record in the topic/partition.
     *
     * @return the timestamp of the record, or -1 if the {{@link #hasTimestamp()}} returns false.
     */
    public long timestamp() {
        return this.timestamp;
    }

    /**
     * 返回这条消息的验证码
     * The checksum (CRC32) of the record.
     *
     * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
     *             computed checksum may not match what was stored on the broker, or what will be returned to the consumer.
     *             It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
     *             message format v2 does not include a record-level checksum (for performance, the record checksum
     *             was replaced with a batch checksum). To maintain compatibility, a partial checksum computed from
     *             the record timestamp, serialized key size, and serialized value size is returned instead, but
     *             this should not be depended on for end-to-end reliability.
     */
    @Deprecated
    public long checksum() {
        // The checksum is null only for message format v2 and above, which do not have a record-level checksum.
        if (checksum == null) {
            this.checksum = DefaultRecord.computePartialChecksum(timestamp, serializedKeySize, serializedValueSize);
        }
        return this.checksum;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the returned
     * size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    /**
     * The topic the record was appended to
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * The partition the record was sent to
     */
    public int partition() {
        return this.topicPartition.partition();
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
