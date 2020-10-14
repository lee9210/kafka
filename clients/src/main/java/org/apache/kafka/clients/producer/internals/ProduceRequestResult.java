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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * 生产者发送之后的结果，每个ProducerBatch中都有个一个实例。
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 */
public class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final TopicPartition topicPartition;

    private volatile Long baseOffset = null;
    private volatile long logAppendTime = RecordBatch.NO_TIMESTAMP;
    private volatile RuntimeException error;

    /**
     * 创建一个实例
     * Create an instance of this class.
     *
     * @param topicPartition The topic and partition to which this record set was sent was sent
     */
    public ProduceRequestResult(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * 记录结果
     *
     * Set the result of the produce request.
     *
     * @param baseOffset The base offset assigned to the record
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param error The error that occurred if there was one, or null
     */
    public void set(long baseOffset, long logAppendTime, RuntimeException error) {
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.error = error;
    }

    /**
     * 标记完成，并唤醒等待的线程
     * Mark this request as complete and unblock any threads waiting on its completion.
     */
    public void done() {
        if (baseOffset == null) {
            throw new IllegalStateException("The method `set` must be invoked before this method.");
        }
        this.latch.countDown();
    }

    /**
     * 让线程等待
     * Await the completion of this request
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * 让线程等待，并设置等待时间
     * Await the completion of this request (up to the given time interval)
     * @param timeout The maximum time to wait
     * @param unit The unit for the max time
     * @return true if the request completed, false if we timed out
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * 请求的baseOffset，batch中的第一条消息的偏移量
     * The base offset for the request (the first offset in the record set)
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * 如果该主题使用日志追加时间，则返回true
     * Return true if log append time is being used for this topic
     */
    public boolean hasLogAppendTime() {
        return logAppendTime != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * 返回日志追加时间或者-1
     * The log append time or -1 if CreateTime is being used
     */
    public long logAppendTime() {
        return logAppendTime;
    }

    /**
     * 获取发送过程中的错误
     * The error thrown (generally on the server) while processing this request
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * 返回topic-partition
     * The topic and partition to which the record was appended
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * 检查request是否完成
     * Has the request completed?
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
