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
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一条记录的发送结果的result，添加了下个包的发送结果，用于链式调用
 * The future result of a record send
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    private final ProduceRequestResult result;
    /** 相对偏移 */
    private final long relativeOffset;
    private final long createTimestamp;
    /** 校验码 */
    private final Long checksum;
    /** key序列化后的大小 */
    private final int serializedKeySize;
    /** value序列化后的大小 */
    private final int serializedValueSize;
    private final Time time;
    /** 下一条FutureRecordMetadata，主要用于链式调用 */
    private volatile FutureRecordMetadata nextRecordMetadata = null;

    public FutureRecordMetadata(ProduceRequestResult result, long relativeOffset, long createTimestamp,
                                Long checksum, int serializedKeySize, int serializedValueSize, Time time) {
        this.result = result;
        this.relativeOffset = relativeOffset;
        this.createTimestamp = createTimestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.time = time;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    /**
     * 获取下一条batch的发送结果
     */
    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        // 等待唤醒
        this.result.await();
        if (nextRecordMetadata != null) {
            // 返回下条batch的发送结果
            return nextRecordMetadata.get();
        }
        // 如果有error就返回
        return valueOrError();
    }

    /**
     * 获取本次发送结果的元数据
     */
    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // Handle overflow.
        long now = time.milliseconds();
        long timeoutMillis = unit.toMillis(timeout);
        long deadline = Long.MAX_VALUE - timeoutMillis < now ? Long.MAX_VALUE : now + timeoutMillis;
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred) {
            throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.");
        }
        if (nextRecordMetadata != null) {
            return nextRecordMetadata.get(deadline - time.milliseconds(), TimeUnit.MILLISECONDS);
        }
        return valueOrError();
    }

    /**
     * 根据分会的future，构造返回数据的链式结构
     * This method is used when we have to split a large batch in smaller ones. A chained metadata will allow the
     * future that has already returned to the users to wait on the newly created split batches even after the
     * old big batch has been deemed as done.
     */
    void chain(FutureRecordMetadata futureRecordMetadata) {
        if (nextRecordMetadata == null) {
            nextRecordMetadata = futureRecordMetadata;
        } else {
            nextRecordMetadata.chain(futureRecordMetadata);
        }
    }

    /**
     * 返回batch发送结果的元数据
     */
    RecordMetadata valueOrError() throws ExecutionException {
        if (this.result.error() != null) {
            throw new ExecutionException(this.result.error());
        } else {
            return value();
        }
    }

    Long checksumOrNull() {
        return this.checksum;
    }

    /**
     * 返回下条batch发送的结果，如果没有的话就构造一个
     * @return
     */
    RecordMetadata value() {
        if (nextRecordMetadata != null) {
            return nextRecordMetadata.value();
        }
        // 构造一个返回数据的实例返回
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.relativeOffset,
                timestamp(), this.checksum, this.serializedKeySize, this.serializedValueSize);
    }

    private long timestamp() {
        return result.hasLogAppendTime() ? result.logAppendTime() : createTimestamp;
    }

    /**
     * 检查本次批量发送是否完成。
     * @return
     */
    @Override
    public boolean isDone() {
        if (nextRecordMetadata != null) {
            return nextRecordMetadata.isDone();
        }
        // 如果本次发送只有一个包，则直接返回本次发送的结果
        return this.result.completed();
    }
}
