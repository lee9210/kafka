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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * 这个类用于在内存中写入新的log数据，也就是说，这是{@link MemoryRecords}的写入路径，它透明地处理压缩并公开添加新记录的方法，可能还包括消息格式转换。
 * 在保持低内存保留率很重要的情况下，在记录追加停止和构建器关闭(例如生成器)之间有一个时间间隔，当前者发生时，调用“closeForRecordAppends”是很重要的。
 * 这将释放压缩缓冲区等资源，这些资源可能比较大(LZ4为64 KB)。
 *
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryRecords}.
 * It transparently handles compression and exposes methods for appending new records, possibly with message
 * format conversion.
 *
 * In cases where keeping memory retention low is important and there's a gap between the time that record appends stop
 * and the builder is closed (e.g. the Producer), it's important to call `closeForRecordAppends` when the former happens.
 * This will release resources like compression buffers that can be relatively large (64 KB for LZ4).
 */
public class MemoryRecordsBuilder implements AutoCloseable {
    /** 压缩率估计因子 */
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    /** 关闭写入 */
    private static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
        }
    });

    /** 时间格式 */
    private final TimestampType timestampType;
    /** 压缩方式 */
    private final CompressionType compressionType;
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    // 用于保存对底层ByteBuffer的引用，以便我们可以写入记录批处理头并访问写入的字节。如果现有的字节缓冲区不够大，ByteBufferOutputStream将分配一个新的字节缓冲区，因此保存对底层字节缓冲区的直接引用是不安全的。
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    /** 初始offset */
    private final int initialPosition;
    private final long baseOffset;
    private final long logAppendTime;
    private final boolean isControlBatch;
    /** partition leader 代数 */
    private final int partitionLeaderEpoch;
    private final int writeLimit;
    private final int batchHeaderSizeInBytes;

    // Use a conservative estimate of the compression ratio. The producer overrides this using statistics
    // from previous batches before appending any records.
    // 使用压缩比的保守估计。在附加任何记录之前，生产者使用来自前一批的统计信息覆盖此数据。
    /** 估计的压缩比 */
    private float estimatedCompressionRatio = 1.0F;

    // Used to append records, may compress data on the fly
    private DataOutputStream appendStream;
    /** 是否是事务 */
    private boolean isTransactional;
    /** 创建者id */
    private long producerId;
    /** 创建者代数 */
    private short producerEpoch;
    /** 基本序列号 */
    private int baseSequence;
    // Number of bytes (excluding the header) written before compression
    /** 压缩前写入的字节数(不包括header) */
    private int uncompressedRecordsSizeInBytes = 0;
    /** record数量 */
    private int numRecords = 0;
    /** 实际压缩比 */
    private float actualCompressionRatio = 1;
    /** 时间格式 */
    private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
    private long offsetOfMaxTimestamp = -1;
    /** 最后一条消息的offset */
    private Long lastOffset = null;
    private Long firstTimestamp = null;

    private MemoryRecords builtRecords;
    private boolean aborted = false;

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        // 验证magic和timestampType
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE) {
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        }
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional) {
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            }
            if (isControlBatch) {
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
            }
            if (compressionType == CompressionType.ZSTD) {
                throw new IllegalArgumentException("ZStandard compression is not supported for magic " + magic);
            }
        }

        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.numRecords = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.isControlBatch = isControlBatch;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialPosition = bufferStream.position();
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compressionType);

        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        this.appendStream = new DataOutputStream(compressionType.wrapForOutput(this.bufferStream, magic));
    }

    /**
     * Construct a new builder.
     *
     * @param buffer The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *               to fit the records appended)
     * @param magic The magic value to use
     * @param compressionType The compression codec to use
     * @param timestampType The desired timestamp type. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset The initial offset to use for
     * @param logAppendTime The log append time of this record set. Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param producerId The producer ID associated with the producer writing this record set
     * @param producerEpoch The epoch of the producer
     * @param baseSequence The sequence number of the first record in this set
     * @param isTransactional Whether or not the records are part of a transaction
     * @param isControlBatch Whether or not this is a control batch (e.g. for transaction markers)
     * @param partitionLeaderEpoch The epoch of the partition leader appending the record set to the log
     * @param writeLimit The desired limit on the total bytes for this record set (note that this can be exceeded
     *                   when compression is used since size estimates are rough, and in the case that the first
     *                   record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer,
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        this(new ByteBufferOutputStream(buffer), magic, compressionType, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public CompressionType compressionType() {
        return compressionType;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    /**
     * 关闭此构建器并返回生成的缓冲区。
     *
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }

    /**
     * 获取最大时间戳及其偏移量。返回的偏移量的细节有点微妙。
     * 如果使用了日志追加时间，则偏移量将是最后一个偏移量，除非没有使用压缩并且消息格式版本为0或1，在这种情况下，它将是第一个偏移量。
     * 如果使用了创建时间，则偏移量将是最后一个偏移量，除非没有使用压缩并且消息格式版本为0或1，在这种情况下，它将是具有最大时间戳的记录的偏移量。
     *
     * Get the max timestamp and its offset. The details of the offset returned are a bit subtle.
     *
     * If the log append time is used, the offset will be the last offset unless no compression is used and
     * the message format version is 0 or 1, in which case, it will be the first offset.
     *
     * If create time is used, the offset will be the last offset unless no compression is used and the message
     * format version is 0 or 1, in which case, it will be the offset of the record with the max timestamp.
     *
     * @return The max timestamp and its offset
     */
    public RecordsInfo info() {
        // 如果时间格式是日志追加事件格式
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            // 如果压缩过，并且magic为2及以上，使用最后一个偏移量
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2) {
                shallowOffsetOfMaxTimestamp = lastOffset;
            // 否则使用初始偏移量
            } else {
                shallowOffsetOfMaxTimestamp = baseOffset;
            }
            // 构造RecordsInfo返回
            return new RecordsInfo(logAppendTime, shallowOffsetOfMaxTimestamp);
        // 如果时间格式不存在，则直接使用最后一个偏移量
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, lastOffset);
        } else {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            // 如果没压缩过，并且magic为2及以上，使用最后一个偏移量
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2) {
                shallowOffsetOfMaxTimestamp = lastOffset;
            // 否则使用最后的偏移量
            } else {
                shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp;
            }
            return new RecordsInfo(maxTimestamp, shallowOffsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    /**
     * 返回batch header(总是未压缩的)和records(压缩前)的大小之和。
     * Return the sum of the size of the batch header (always uncompressed) and the records (before compression).
     */
    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    /**
     * 设置producer状态
     * @param producerId
     * @param producerEpoch
     * @param baseSequence
     * @param isTransactional
     */
    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            // Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            // If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            // be re queued. In this case, we should not attempt to set the state again, since changing the producerId and sequence
            // once a batch has been sent to the broker risks introducing duplicates.
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    /**
     * 设置lastOffset
     * @param lastOffset
     */
    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null) {
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        }
        this.lastOffset = lastOffset;
    }

    /**
     * 释放记录追加所需的资源(例如压缩缓冲区)。把数据刷到内存中
     * 调用此方法后，只能更新 RecordBatch header。
     *
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                appendStream = CLOSED_STREAM;
            }
        }
    }

    /**
     * 设置此builder阻塞
     */
    public void abort() {
        closeForRecordAppends();
        buffer().position(initialPosition);
        aborted = true;
    }

    /**
     * 重新打开并且设置可重新写
     */
    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (aborted) {
            throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        }
        builtRecords = null;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    /**
     * 关闭此builder
     */
    @Override
    public void close() {
        // 如果已经阻塞，则抛出异常
        if (aborted) {
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");
        }

        // 如果已经存在，则直接返回
        if (builtRecords != null) {
            return;
        }
        // 验证producer状态
        validateProducerState();
        // 释放底层资源
        closeForRecordAppends();
        // 如果记录为空
        if (numRecords == 0L) {
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            } else if (compressionType != CompressionType.NONE) {
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;
            }

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    /**
     * 验证producer状态
     */
    private void validateProducerState() {
        // 如果是事务，并且没有producerId,直接抛出异常
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID) {
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");
        }
        // 如果没有设置producerId
        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH) {
                throw new IllegalArgumentException("Invalid negative producer epoch");
            }

            if (baseSequence < 0 && !isControlBatch) {
                throw new IllegalArgumentException("Invalid negative sequence number used");
            }

            if (magic < RecordBatch.MAGIC_VALUE_V2) {
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
            }
        }
    }

    /**
     * Write the header to the default batch.
     * @return the written compressed bytes.
     */
    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);
        int size = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            maxTimestamp = logAppendTime;
        } else {
            maxTimestamp = this.maxTimestamp;
        }

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                firstTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * 将header写入到records中，返回压缩后的字节数
     * Write the header to the legacy batch.
     * @return the written compressed bytes.
     */
    private int writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);

        int wrapperSize = pos - initialPosition - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * 对版本为0或1追加一条记录并返回其checksum，对版本2及以上格式返回null。
     * Append a record and return its checksum for message format v0 and v1, or null for v2 and above.
     */
    private Long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                  ByteBuffer value, Header[] headers) {
        try {
            if (isControlRecord != isControlBatch) {
                throw new IllegalArgumentException("Control records can only be appended to control batches");
            }

            if (lastOffset != null && offset <= lastOffset) {
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s " +
                        "(Offsets must increase monotonically).", offset, lastOffset));
            }

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP) {
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);
            }

            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0) {
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");
            }

            if (firstTimestamp == null) {
                firstTimestamp = timestamp;
            }

            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
                return null;
            } else {
                return appendLegacyRecord(offset, timestamp, key, value, magic);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * 在给定的偏移位置追加一个新record。
     *
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * 在给定的偏移位置追加一个新record。
     *
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * 在给定的偏移位置追加一个新record。
     *
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        return appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * 在给定的偏移位置追加一个新record。
     *
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        return appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 在给定的偏移位置追加一个新record。
     *
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, SimpleRecord record) {
        return appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * 在给定的偏移位置追加一个控制记录。
     * 控制记录类型必须是已知的，否则此方法将引发错误。
     *
     * Append a control record at the given offset. The control record type must be known or
     * this method will raise an error.
     *
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendControlRecordWithOffset(long offset, SimpleRecord record) {
        short typeId = ControlRecordType.parseTypeId(record.key());
        ControlRecordType type = ControlRecordType.fromTypeId(typeId);
        if (type == ControlRecordType.UNKNOWN) {
            throw new IllegalArgumentException("Cannot append record with unknown control record type " + typeId);
        }

        return appendWithOffset(offset, true, record.timestamp(),
            record.key(), record.value(), record.headers());
    }

    /**
     * 在下一个连续偏移处追加一个新record。
     *
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value) {
        return append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 在下一个连续偏移处追加一个新record。
     *
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * 在下一个连续偏移处追加一个新record。
     *
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * 在下一个连续偏移处追加一个新record。
     *
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * 在下一个连续偏移处追加一个新record。
     *
     * Append a new record at the next sequential offset.
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(SimpleRecord record) {
        return appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * 在下一个顺序偏移处追加一个control record。
     *
     * Append a control record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param type The control record type (cannot be UNKNOWN)
     * @param value The control record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    private Long appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct keyStruct = type.recordKey();
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        return appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * 如果不支持消息格式,则返回记录或null的CRC值
     *
     * Return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID) {
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        }
        if (!isTransactional) {
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        }
        ByteBuffer value = marker.serializeValue();
        return appendControlRecord(timestamp, marker.controlType(), value);
    }

    /**
     * 如果不支持消息格式,则返回记录或null的CRC
     *
     * Return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendLeaderChangeMessage(long timestamp, LeaderChangeMessage leaderChangeMessage) {
        if (partitionLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            throw new IllegalArgumentException("Partition leader epoch must be valid, but get " + partitionLeaderEpoch);
        }

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = leaderChangeMessage.size(cache, ControlRecordUtils.LEADER_CHANGE_SCHEMA_VERSION);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
        leaderChangeMessage.write(accessor, cache, ControlRecordUtils.LEADER_CHANGE_SCHEMA_VERSION);
        buffer.flip();
        return appendControlRecord(timestamp, ControlRecordType.LEADER_CHANGE, buffer);
    }

    /**
     * 添加一个粘粘record而不做offset/magic验证(这应该只在测试中使用)。
     *
     * Add a legacy record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * 添加一个粘粘record而不做offset/magic验证(这应该只在测试中使用)。
     *
     * Append a record without doing offset/magic validation (this should only be used in testing).
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, SimpleRecord record) throws IOException {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int offsetDelta = (int) (offset - baseOffset);
            long timestamp = record.timestamp();
            if (firstTimestamp == null) {
                firstTimestamp = timestamp;
            }

            int sizeInBytes = DefaultRecord.writeTo(appendStream,
                offsetDelta,
                timestamp - firstTimestamp,
                record.key(),
                record.value(),
                record.headers());
            recordWritten(offset, timestamp, sizeInBytes);
        } else {
            LegacyRecord legacyRecord = LegacyRecord.create(magic,
                record.timestamp(),
                Utils.toNullableArray(record.key()),
                Utils.toNullableArray(record.value()));
            appendUncheckedWithOffset(offset, legacyRecord);
        }
    }

    /**
     * 在下一个连续偏移处追加一条record。
     *
     * Append a record at the next sequential offset.
     * @param record the record to add
     */
    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * 使用不同的偏移量追加record
     *
     * Append a log record using a different offset
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * 添加一个具有给定偏移量的记录。这个记录必须有一个与用来构造这个构建器的magic相匹配的magic，并且偏移量必须大于最后一个附加的记录。
     *
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended record.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    /**
     * 将记录追加到下一个连续偏移量。如果还没有附加任何record，则使用此构建器的基偏移量。
     *
     * Append the record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     * @param record The record to add
     */
    public void append(LegacyRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - firstTimestamp;
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }

    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, byte magic) throws IOException {
        ensureOpenForRecordAppend();
        if (compressionType == CompressionType.NONE && timestampType == TimestampType.LOG_APPEND_TIME) {
            timestamp = logAppendTime;
        }

        int size = LegacyRecord.recordSize(magic, key, value);
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            timestamp = logAppendTime;
        }
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    private long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compressionType != CompressionType.NONE) {
            return offset - baseOffset;
        }
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        if (numRecords == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        }
        if (offset - baseOffset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset +
                    ", last offset: " + offset);
        }

        numRecords += 1;
        uncompressedRecordsSizeInBytes += size;
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM) {
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
        }
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed()) {
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        }
        if (aborted) {
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
        }
    }

    /**
     * 获得写入的字节数的估计(基于硬编码在{@link CompressionType}中的估计因子)
     *
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    private int estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedRecordsSizeInBytes;
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return batchHeaderSizeInBytes + (int) (uncompressedRecordsSizeInBytes * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * 为内存记录生成器设置估计的压缩比。
     *
     * Set the estimated compression ratio for the memory records builder.
     */
    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    /**
     * 检查剩余空间是否足够写入
     * 检查是否为包含给定键/值对的新记录留有空间。如果没有添加任何记录，则返回true。
     *
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * 检查是否为包含给定键/值对的新记录留有空间。如果没有添加任何记录，则返回true。
     *
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     */
    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        if (isFull()) {
            return false;
        }

        // We always allow at least one record to be appended (the ByteBufferOutputStream will grow as needed)
        if (numRecords == 0) {
            return true;
        }

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = firstTimestamp == null ? 0 : timestamp - firstTimestamp;
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        // Be conservative and not take compression of the new record into consideration.
        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    /**
     * 获取写入底层缓冲区的字节数的估计值。如果记录集没有被压缩或者构建器已经被关闭，那么返回的值是完全正确的。
     *
     * Get an estimate of the number of bytes written to the underlying buffer. The returned value
     * is exactly correct if the record set is not compressed or if the builder has been closed.
     */
    public int estimatedSizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    private long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
    }

    /**
     * 消息格式封装
     */
    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                           long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    /**
     * Return the producer id of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}
