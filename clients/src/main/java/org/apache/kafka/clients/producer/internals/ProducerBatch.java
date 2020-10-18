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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * 正在发送或将要发送的一批记录。
 * 这个类不是线程安全的，在修改它时必须使用外部同步
 *
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    /** 发送的状态 */
    private enum FinalState { ABORTED, FAILED, SUCCEEDED }
    /** 创建时间 */
    final long createdMs;
    /** 属于Topic和Partition的包装类 */
    final TopicPartition topicPartition;
    /** 标识RecordBatch状态的Future对象，主要用来保存发送结果信息 */
    final ProduceRequestResult produceFuture;
    /** 发送请求的回调list */
    private final List<Thunk> thunks = new ArrayList<>();
    /** 基于内存的record构造器，可以通过此builder方便的写入数据等操作 */
    private final MemoryRecordsBuilder recordsBuilder;
    private final AtomicInteger attempts = new AtomicInteger(0);
    private final boolean isSplitBatch;
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);

    int recordCount;
    int maxRecordSize;
    private long lastAttemptMs;
    private long lastAppendTime;
    private long drainedMs;
    private boolean retry;
    private boolean reopened;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        this.createdMs = createdMs;
        this.lastAttemptMs = createdMs;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compressionType());
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * 将记录追加到当前记录集中，并返回该记录集中的相对偏移量
     *
     * Append the record to the current record set and return the relative offset within that record set
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 检查builder剩余空间是否足够写入
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        // 足够写入
        } else {
            // 往builder中写入数据，获得checksum
            Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
            // 设置builder中的数量
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            // 更新最后增加时间
            this.lastAppendTime = now;
            // 构造Future函数
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length,
                                                                   Time.SYSTEM);
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            // 把future和callback封装进去
            thunks.add(new Thunk(callback, future));
            // record数量加1
            this.recordCount++;
            // 返回future
            return future;
        }
    }

    /**
     * 往builder中写入一条record
     * 这种方法只被{@link #split(int)}在将大批拆分为小批时使用。
     *
     * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
     * @return true if the record has been successfully appended, false otherwise.
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        // 检查builder剩余空间是否足够写入
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // No need to get the CRC.
            // 往builder中写入数据
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, thunk.future.checksumOrNull(),
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining(),
                                                                   Time.SYSTEM);
            // Chain the future to the original thunk.
            // 把future绑定在thunk链上
            thunk.future.chain(future);
            this.thunks.add(thunk);
            this.recordCount++;
            return true;
        }
    }

    /**
     * 中止batch并完成future和回调。
     * Abort the batch and complete the future and callbacks.
     *
     * @param exception The exception to use to complete the future and awaiting callbacks.
     */
    public void abort(RuntimeException exception) {
        if (!finalState.compareAndSet(null, FinalState.ABORTED)) {
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());
        }

        log.trace("Aborting batch for partition {}", topicPartition, exception);
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, exception);
    }

    /**
     * Return `true` if {@link #done(long, long, RuntimeException)} has been invoked at least once, `false` otherwise.
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be called
     * once or twice on a batch. It may be called twice if
     * 1. An inflight batch expires before a response from the broker is received. The batch's final
     * state is set to FAILED. But it could succeed on the broker and second time around batch.done() may
     * try to set SUCCEEDED final state.
     * 2. If a transaction abortion happens or if the producer is closed forcefully, the final state is
     * ABORTED but again it could succeed if broker responds with a success.
     *
     * Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
     * Attempted transitions from one failure state to the same or a different failed state are ignored.
     * Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     * @return true if the batch was completed successfully and false if the batch was previously aborted
     */
    public boolean done(long baseOffset, long logAppendTime, RuntimeException exception) {
        final FinalState tryFinalState = (exception == null) ? FinalState.SUCCEEDED : FinalState.FAILED;

        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, exception);
        }

        if (this.finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, exception);
            return true;
        }

        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // Log if a previously unsuccessful batch succeeded later on.
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                    tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                    this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            // A SUCCESSFUL batch must not attempt another state change.
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    /**
     * 完成future和调用回调
     */
    private void completeFutureAndFireCallbacks(long baseOffset, long logAppendTime, RuntimeException exception) {
        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        // 在调用回调函数之前对future进行设置，因为我们依赖它的状态来进行' onCompletion '调用
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        // 执行callback调用
        for (Thunk thunk : thunks) {
            try {
                // 如果没有发生异常
                if (exception == null) {
                    // 获取发送结果记录
                    RecordMetadata metadata = thunk.future.value();
                    if (thunk.callback != null) {
                        // 调用onCompletion
                        thunk.callback.onCompletion(metadata, null);
                    }
                // 发生异常
                } else {
                    if (thunk.callback != null) {
                        thunk.callback.onCompletion(null, exception);
                    }
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }
        // 本次发送完成，唤醒下条发送结果处理线程
        produceFuture.done();
    }

    /**
     * 把一个batch分解成多个batch
     * @param splitBatchSize 为分解后每个batch的大小
     * @return 保存batch的queue
     */
    public Deque<ProducerBatch> split(int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        // 获取builder的记录的数据
        MemoryRecords memoryRecords = recordsBuilder.build();
        // 获取迭代
        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        // 如果为空，则抛出异常
        if (!recordBatchIter.hasNext()) {
            throw new IllegalStateException("Cannot split an empty producer batch.");
        }
        // 获取第一个batch，进行验证(由于此builder只有一个batch，只需要取出第一个就行了)
        RecordBatch recordBatch = recordBatchIter.next();
        // 不支持magic为0和1的版本
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed()) {
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");
        }
        // 验证是否只有一个
        if (recordBatchIter.hasNext()) {
            throw new IllegalArgumentException("A producer batch should only have one record batch.");
        }
        // 获取thunk迭代器
        Iterator<Thunk> thunkIter = thunks.iterator();
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        ProducerBatch batch = null;
        // 迭代Record
        int i =0;
        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            // 获取thunk
            Thunk thunk = thunkIter.next();
            // 如果batch为空，则创建一个新的batch
            if (batch == null) {
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
            }
            i ++;
            // A newly created batch can always host the first message.
            // 如果不能加进去，则表示此次batch已经满了，需要把batch加到queue中，并关闭batch的写入
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                batches.add(batch);
                // 关闭batch的写入
                batch.closeForRecordAppends();
                // 重新创建一个batch的builder
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                // 写入第一条
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // Close the last batch and add it to the batch list after split.
        // 由于最后一个batch没有关闭和加入queue中，加入后关闭写入
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, new RecordBatchTooLargeException());
        // 唤醒producer的写线程完成
        produceFuture.done();

        // todo sequence
        if (hasSequence()) {
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                sequence += newBatch.recordCount;
            }
        }
        return batches;
    }

    /**
     * 为record创建batch累加器,主要是创建一个新的builder
     */
    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        // 预估最大值
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compressionType(), record.key(), record.value(), record.headers()), batchSize);
        // 创建空buffer
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // Note that we intentionally do not set producer state (producerId, epoch, sequence, and isTransactional)
        // for the newly created batch. This will be set when the batch is dequeued for sending (which is consistent
        // with how normal batches are handled).
        // 创建一个新的builder
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compressionType(),
                TimestampType.CREATE_TIME, 0L);
        // 创建一个新的batch返回
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    public boolean isCompressed() {
        return recordsBuilder.compressionType() != CompressionType.NONE;
    }

    /**
     * 和回调相关的包装类
     * 主要包装回调和发送结果对象
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * 检查是否已经超过时间
     */
    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    /**
     * 获取状态
     */
    public FinalState finalState() {
        return this.finalState.get();
    }

    int attempts() {
        return attempts.get();
    }

    void reenqueued(long now) {
        attempts.getAndIncrement();
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        retry = true;
    }

    long queueTimeMs() {
        return drainedMs - createdMs;
    }

    long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * 重新设置producer状态
     * @param producerIdAndEpoch
     * @param baseSequence
     * @param isTransactional
     */
    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        reopened = true;
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * 关闭builder，只能设置header
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    public void close() {
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compressionType(),
                                                       (float) recordsBuilder.compressionRatio());
        }
        reopened = false;
    }

    /**
     * Abort the record builder and reset the state of the underlying buffer. This is used prior to aborting
     * the batch with {@link #abort(RuntimeException)} and ensures that no record previously appended can be
     * read. This is used in scenarios where we want to ensure a batch ultimately gets aborted, but in which
     * it is not safe to invoke the completion callbacks (e.g. because we are holding a lock,
     * {@link RecordAccumulator#abortBatches()}).
     */
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    public int lastSequence() {
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    public boolean hasSequence() {
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }
}
