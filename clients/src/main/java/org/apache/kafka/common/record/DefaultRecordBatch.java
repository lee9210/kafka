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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Crc32C;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * magic 2 以上的RecordBatch默认实现
 * RecordBatch implementation for magic 2 and above. The schema is given below:
 *
 * RecordBatch =>
 *  BaseOffset => Int64
 *  Length => Int32
 *  PartitionLeaderEpoch => Int32
 *  Magic => Int8
 *  CRC => Uint32
 *  Attributes => Int16
 *  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
 *  FirstTimestamp => Int64
 *  MaxTimestamp => Int64
 *  ProducerId => Int64
 *  ProducerEpoch => Int16
 *  BaseSequence => Int32
 *  Records => [Record]
 *
 * Note that when compression is enabled (see attributes below), the compressed record data is serialized
 * directly following the count of the number of records.
 *
 * The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC). It is
 * located after the magic byte, which means that clients must parse the magic byte before deciding how to interpret
 * the bytes between the batch length and the magic byte. The partition leader epoch field is not included in the CRC
 * computation to avoid the need to recompute the CRC when this field is assigned for every batch that is received by
 * the broker. The CRC-32C (Castagnoli) polynomial is used for the computation.
 *
 * On Compaction: Unlike the older message formats, magic v2 and above preserves the first and last offset/sequence
 * numbers from the original batch when the log is cleaned. This is required in order to be able to restore the
 * producer's state when the log is reloaded. If we did not retain the last sequence number, then following
 * a partition leader failure, once the new leader has rebuilt the producer state from the log, the next sequence
 * expected number would no longer be in sync with what was written by the client. This would cause an
 * unexpected OutOfOrderSequence error, which is typically fatal. The base sequence number must be preserved for
 * duplicate checking: the broker checks incoming Produce requests for duplicates by verifying that the first and
 * last sequence numbers of the incoming batch match the last from that producer.
 *
 * Note that if all of the records in a batch are removed during compaction, the broker may still retain an empty
 * batch header in order to preserve the producer sequence information as described above. These empty batches
 * are retained only until either a new sequence number is written by the corresponding producer or the producerId
 * is expired from lack of activity.
 *
 * There is no similar need to preserve the timestamp from the original batch after compaction. The FirstTimestamp
 * field therefore always reflects the timestamp of the first record in the batch. If the batch is empty, the
 * FirstTimestamp will be set to -1 (NO_TIMESTAMP).
 *
 * Similarly, the MaxTimestamp field reflects the maximum timestamp of the current records if the timestamp type
 * is CREATE_TIME. For LOG_APPEND_TIME, on the other hand, the MaxTimestamp field reflects the timestamp set
 * by the broker and is preserved after compaction. Additionally, the MaxTimestamp of an empty batch always retains
 * the previous value prior to becoming empty.
 *
 * The current attributes are given below:
 *
 *  -------------------------------------------------------------------------------------------------
 *  | Unused (6-15) | Control (5) | Transactional (4) | Timestamp Type (3) | Compression Type (0-2) |
 *  -------------------------------------------------------------------------------------------------
 */
public class DefaultRecordBatch extends AbstractRecordBatch implements MutableRecordBatch {
    static final int BASE_OFFSET_OFFSET = 0;
    static final int BASE_OFFSET_LENGTH = 8;
    static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int LENGTH_LENGTH = 4;
    static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
    static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
    static final int MAGIC_LENGTH = 1;
    static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int CRC_LENGTH = 4;
    static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    public static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    static final int FIRST_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int FIRST_TIMESTAMP_LENGTH = 8;
    static final int MAX_TIMESTAMP_OFFSET = FIRST_TIMESTAMP_OFFSET + FIRST_TIMESTAMP_LENGTH;
    static final int MAX_TIMESTAMP_LENGTH = 8;
    static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
    static final int PRODUCER_ID_LENGTH = 8;
    static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
    static final int PRODUCER_EPOCH_LENGTH = 2;
    static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
    static final int BASE_SEQUENCE_LENGTH = 4;
    public static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
    static final int RECORDS_COUNT_LENGTH = 4;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    public static final int RECORD_BATCH_OVERHEAD = RECORDS_OFFSET;

    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
    private static final int CONTROL_FLAG_MASK = 0x20;
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    private static final int MAX_SKIP_BUFFER_SIZE = 2048;

    private final ByteBuffer buffer;

    DefaultRecordBatch(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * 获取magic值
     */
    @Override
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * 如果checksum无效，则抛出异常。
     */
    @Override
    public void ensureValid() {
        // 总长小于overhead
        if (sizeInBytes() < RECORD_BATCH_OVERHEAD) {
            throw new CorruptRecordException("Record batch is corrupt (the size " + sizeInBytes() + " is smaller than the minimum allowed overhead " + RECORD_BATCH_OVERHEAD + ")");
        }
        // 检查checksum
        if (!isValid()) {
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum() + ", computed crc = " + computeChecksum() + ")");
        }
    }

    /**
     * 获取此batch中第一个record的时间戳。
     * 它始终是record的创建时间，即使batch的时间戳类型是日志追加时间。
     * Get the timestamp of the first record in this batch. It is always the create time of the record even if the
     * timestamp type of the batch is log append time.
     *
     * @return The first timestamp or {@link RecordBatch#NO_TIMESTAMP} if the batch is empty
     */
    public long firstTimestamp() {
        return buffer.getLong(FIRST_TIMESTAMP_OFFSET);
    }

    /**
     * 获取此记录批处理的最大时间戳或日志追加时间。
     */
    @Override
    public long maxTimestamp() {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET);
    }

    /**
     * 获取此record batch的时间戳类型。
     */
    @Override
    public TimestampType timestampType() {
        return (attributes() & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }

    /**
     * 获取此record batch中的baseOffset。
     * 对于2之前的magic版本，基偏移量始终是批处理中第一个消息的偏移量。 这通常需要深度迭代，并将返回记录批处理中第一个记录的偏移量。
     * 对于magic version 2及以上版本，这将返回原始记录批处理的第一个偏移量(即压缩之前)。
     */
    @Override
    public long baseOffset() {
        return buffer.getLong(BASE_OFFSET_OFFSET);
    }

    /**
     * 获取此record batch中的最后偏移量。
     */
    @Override
    public long lastOffset() {
        // 基础增量加上最后偏量
        return baseOffset() + lastOffsetDelta();
    }

    /**
     * 获取producerId
     */
    @Override
    public long producerId() {
        return buffer.getLong(PRODUCER_ID_OFFSET);
    }

    /**
     * 获取producer版本号
     */
    @Override
    public short producerEpoch() {
        return buffer.getShort(PRODUCER_EPOCH_OFFSET);
    }

    /**
     * 获取record batch的序列号
     */
    @Override
    public int baseSequence() {
        return buffer.getInt(BASE_SEQUENCE_OFFSET);
    }

    /**
     * 获取最后一条record的offset
     */
    private int lastOffsetDelta() {
        return buffer.getInt(LAST_OFFSET_DELTA_OFFSET);
    }

    /**
     * 获取record batch的最后一条序列号
     */
    @Override
    public int lastSequence() {
        // 获取第一条序列号
        int baseSequence = baseSequence();
        // 如果没有则返回-1
        if (baseSequence == RecordBatch.NO_SEQUENCE) {
            return RecordBatch.NO_SEQUENCE;
        }
        // 返回增加量
        return incrementSequence(baseSequence, lastOffsetDelta());
    }

    /**
     * 获取压缩方式
     */
    @Override
    public CompressionType compressionType() {
        return CompressionType.forId(attributes() & COMPRESSION_CODEC_MASK);
    }

    /**
     * 获取此batch的大小(以字节为单位)，包括record的大小和batch overhead。
     */
    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + buffer.getInt(LENGTH_OFFSET);
    }

    /**
     * 获取record的数量
     */
    private int count() {
        return buffer.getInt(RECORDS_COUNT_OFFSET);
    }

    /**
     * 获取record的数量
     */
    @Override
    public Integer countOrNull() {
        return count();
    }

    /**
     * 把此buffer复制一份，写入到传入buffer中
     * @param buffer The buffer to write the batch to
     */
    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(this.buffer.duplicate());
    }

    /**
     * 把此buffer复制一份，写入到outputStream中
     * @param outputStream The buffer to write the batch to
     */
    @Override
    public void writeTo(ByteBufferOutputStream outputStream) {
        outputStream.write(this.buffer.duplicate());
    }

    /**
     * 检查是否是事务
     */
    @Override
    public boolean isTransactional() {
        return (attributes() & TRANSACTIONAL_FLAG_MASK) > 0;
    }

    /**
     * 检查这是否是一个控制batch(即，是否在batch属性中设置了控制位)。
     * 对于2之前的magic，false。
     * @return
     */
    @Override
    public boolean isControlBatch() {
        return (attributes() & CONTROL_FLAG_MASK) > 0;
    }

    /**
     * 获取partition leader的版本号
     * @return
     */
    @Override
    public int partitionLeaderEpoch() {
        return buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET);
    }

    /**
     * 获取record的迭代器
     */
    private CloseableIterator<Record> compressedIterator(BufferSupplier bufferSupplier, boolean skipKeyValue) {
        // 复制一份
        final ByteBuffer buffer = this.buffer.duplicate();
        // 定位开始位置
        buffer.position(RECORDS_OFFSET);
        // 构造inputStream
        final DataInputStream inputStream = new DataInputStream(compressionType().wrapForInput(buffer, magic(),
            bufferSupplier));

        if (skipKeyValue) {
            // this buffer is used to skip length delimited fields like key, value, headers
            // 此缓冲区用于跳过以长度分隔的字段，例如key, value, headers
            byte[] skipArray = new byte[MAX_SKIP_BUFFER_SIZE];

            return new StreamRecordIterator(inputStream) {
                @Override
                protected Record doReadRecord(long baseOffset, long firstTimestamp, int baseSequence, Long logAppendTime) throws IOException {
                    return DefaultRecord.readPartiallyFrom(inputStream, skipArray, baseOffset, firstTimestamp, baseSequence, logAppendTime);
                }
            };
        } else {
            return new StreamRecordIterator(inputStream) {
                @Override
                protected Record doReadRecord(long baseOffset, long firstTimestamp, int baseSequence, Long logAppendTime) throws IOException {
                    return DefaultRecord.readFrom(inputStream, baseOffset, firstTimestamp, baseSequence, logAppendTime);
                }
            };
        }
    }

    /**
     * 获取record的未经压缩的迭代器
     * @return
     */
    private CloseableIterator<Record> uncompressedIterator() {
        final ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);
        return new RecordIterator() {
            @Override
            protected Record readNext(long baseOffset, long firstTimestamp, int baseSequence, Long logAppendTime) {
                try {
                    return DefaultRecord.readFrom(buffer, baseOffset, firstTimestamp, baseSequence, logAppendTime);
                } catch (BufferUnderflowException e) {
                    throw new InvalidRecordException("Incorrect declared batch size, premature EOF reached");
                }
            }
            @Override
            protected boolean ensureNoneRemaining() {
                return !buffer.hasRemaining();
            }
            @Override
            public void close() {}
        };
    }

    /**
     * 获取record迭代器
     */
    @Override
    public Iterator<Record> iterator() {
        if (count() == 0) {
            return Collections.emptyIterator();
        }
        // 如果压缩过，则创建解压缩后的迭代器
        if (!isCompressed()) {
            return uncompressedIterator();
        }

        // for a normal iterator, we cannot ensure that the underlying compression stream is closed,
        // so we decompress the full record set here. Use cases which call for a lower memory footprint
        // can use `streamingIterator` at the cost of additional complexity
        // 创建迭代器
        try (CloseableIterator<Record> iterator = compressedIterator(BufferSupplier.NO_CACHING, false)) {
            // 用list来存储
            List<Record> records = new ArrayList<>(count());
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
            // 返回迭代器
            return records.iterator();
        }
    }

    /**
     * 获取record迭代器
     * key和value都为空
     */
    @Override
    public CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier) {
        if (count() == 0) {
            return CloseableIterator.wrap(Collections.emptyIterator());
        }

        /*
         * For uncompressed iterator, it is actually not worth skipping key / value / headers at all since
         * its ByteBufferInputStream's skip() function is less efficient compared with just reading it actually
         * as it will allocate new byte array.
         */
        if (!isCompressed()) {
            return uncompressedIterator();
        }

        // we define this to be a closable iterator so that caller (i.e. the log validator) needs to close it
        // while we can save memory footprint of not decompressing the full record set ahead of time
        return compressedIterator(bufferSupplier, true);
    }

    /**
     * 返回一个流迭代器
     */
    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        if (isCompressed()) {
            return compressedIterator(bufferSupplier, false);
        } else {
            return uncompressedIterator();
        }
    }

    /**
     * 设置此batch的最后偏移量。
     * @param offset The last offset to use
     */
    @Override
    public void setLastOffset(long offset) {
        buffer.putLong(BASE_OFFSET_OFFSET, offset - lastOffsetDelta());
    }

    /**
     * 设置此batch处理的最大时间戳
     */
    @Override
    public void setMaxTimestamp(TimestampType timestampType, long maxTimestamp) {
        long currentMaxTimestamp = maxTimestamp();
        // We don't need to recompute crc if the timestamp is not updated.
        if (timestampType() == timestampType && currentMaxTimestamp == maxTimestamp) {
            return;
        }

        byte attributes = computeAttributes(compressionType(), timestampType, isTransactional(), isControlBatch());
        buffer.putShort(ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(MAX_TIMESTAMP_OFFSET, maxTimestamp);
        long crc = computeChecksum();
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);
    }

    /**
     * 为这个batch设置partition leader版本号
     * @param epoch The partition leader epoch to use
     */
    @Override
    public void setPartitionLeaderEpoch(int epoch) {
        buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    /**
     * 获取checksum
     */
    @Override
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * 检查这batch的checksum是否正确
     */
    @Override
    public boolean isValid() {
        return sizeInBytes() >= RECORD_BATCH_OVERHEAD && checksum() == computeChecksum();
    }

    /**
     * 计算checksum
     * @return
     */
    private long computeChecksum() {
        return Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET);
    }

    /**
     * 获取attribute
     * @return
     */
    private byte attributes() {
        // note we're not using the second byte of attributes
        return (byte) buffer.getShort(ATTRIBUTES_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultRecordBatch that = (DefaultRecordBatch) o;
        return Objects.equals(buffer, that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    /**
     * 计算attribute
     */
    private static byte computeAttributes(CompressionType type, TimestampType timestampType,
                                          boolean isTransactional, boolean isControl) {
        if (timestampType == TimestampType.NO_TIMESTAMP_TYPE) {
            throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for message " +
                    "format v2 and above");
        }

        byte attributes = isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
        if (isControl) {
            attributes |= CONTROL_FLAG_MASK;
        }
        if (type.id > 0) {
            attributes |= COMPRESSION_CODEC_MASK & type.id;
        }
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            attributes |= TIMESTAMP_TYPE_MASK;
        }
        return attributes;
    }

    /**
     * buffer中写入空header
     */
    public static void writeEmptyHeader(ByteBuffer buffer,
                                        byte magic,
                                        long producerId,
                                        short producerEpoch,
                                        int baseSequence,
                                        long baseOffset,
                                        long lastOffset,
                                        int partitionLeaderEpoch,
                                        TimestampType timestampType,
                                        long timestamp,
                                        boolean isTransactional,
                                        boolean isControlRecord) {
        int offsetDelta = (int) (lastOffset - baseOffset);
        writeHeader(buffer, baseOffset, offsetDelta, DefaultRecordBatch.RECORD_BATCH_OVERHEAD, magic,
                CompressionType.NONE, timestampType, RecordBatch.NO_TIMESTAMP, timestamp, producerId,
                producerEpoch, baseSequence, isTransactional, isControlRecord, partitionLeaderEpoch, 0);
    }

    /**
     * 往buffer中写header
     */
    static void writeHeader(ByteBuffer buffer,
                            long baseOffset,
                            int lastOffsetDelta,
                            int sizeInBytes,
                            byte magic,
                            CompressionType compressionType,
                            TimestampType timestampType,
                            long firstTimestamp,
                            long maxTimestamp,
                            long producerId,
                            short epoch,
                            int sequence,
                            boolean isTransactional,
                            boolean isControlBatch,
                            int partitionLeaderEpoch,
                            int numRecords) {
        if (magic < RecordBatch.CURRENT_MAGIC_VALUE) {
            throw new IllegalArgumentException("Invalid magic value " + magic);
        }
        if (firstTimestamp < 0 && firstTimestamp != NO_TIMESTAMP) {
            throw new IllegalArgumentException("Invalid message timestamp " + firstTimestamp);
        }

        short attributes = computeAttributes(compressionType, timestampType, isTransactional, isControlBatch);

        int position = buffer.position();
        buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset);
        buffer.putInt(position + LENGTH_OFFSET, sizeInBytes - LOG_OVERHEAD);
        buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch);
        buffer.put(position + MAGIC_OFFSET, magic);
        buffer.putShort(position + ATTRIBUTES_OFFSET, attributes);
        buffer.putLong(position + FIRST_TIMESTAMP_OFFSET, firstTimestamp);
        buffer.putLong(position + MAX_TIMESTAMP_OFFSET, maxTimestamp);
        buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
        buffer.putLong(position + PRODUCER_ID_OFFSET, producerId);
        buffer.putShort(position + PRODUCER_EPOCH_OFFSET, epoch);
        buffer.putInt(position + BASE_SEQUENCE_OFFSET, sequence);
        buffer.putInt(position + RECORDS_COUNT_OFFSET, numRecords);
        long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
        buffer.putInt(position + CRC_OFFSET, (int) crc);
        buffer.position(position + RECORD_BATCH_OVERHEAD);
    }

    @Override
    public String toString() {
        return "RecordBatch(magic=" + magic() + ", offsets=[" + baseOffset() + ", " + lastOffset() + "], " +
                "sequence=[" + baseSequence() + ", " + lastSequence() + "], " +
                "isTransactional=" + isTransactional() + ", isControlBatch=" + isControlBatch() + ", " +
                "compression=" + compressionType() + ", timestampType=" + timestampType() + ", crc=" + checksum() + ")";
    }

    public static int sizeInBytes(long baseOffset, Iterable<Record> records) {
        Iterator<Record> iterator = records.iterator();
        if (!iterator.hasNext()) {
            return 0;
        }

        int size = RECORD_BATCH_OVERHEAD;
        Long firstTimestamp = null;
        while (iterator.hasNext()) {
            Record record = iterator.next();
            int offsetDelta = (int) (record.offset() - baseOffset);
            if (firstTimestamp == null) {
                firstTimestamp = record.timestamp();
            }
            long timestampDelta = record.timestamp() - firstTimestamp;
            size += DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    public static int sizeInBytes(Iterable<SimpleRecord> records) {
        Iterator<SimpleRecord> iterator = records.iterator();
        if (!iterator.hasNext()) {
            return 0;
        }

        int size = RECORD_BATCH_OVERHEAD;
        int offsetDelta = 0;
        Long firstTimestamp = null;
        while (iterator.hasNext()) {
            SimpleRecord record = iterator.next();
            if (firstTimestamp == null) {
                firstTimestamp = record.timestamp();
            }
            long timestampDelta = record.timestamp() - firstTimestamp;
            size += DefaultRecord.sizeInBytes(offsetDelta++, timestampDelta, record.key(), record.value(),
                    record.headers());
        }
        return size;
    }

    /**
     * 使用给定的键和值获取只有单个record batch的大小上限。
     * 这只是一个估计，因为它没有考虑到来自所使用的压缩算法的额外开销。
     *
     * Get an upper bound on the size of a batch with only a single record using a given key and value. This
     * is only an estimate because it does not take into account additional overhead from the compression
     * algorithm used.
     */
    static int estimateBatchSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        return RECORD_BATCH_OVERHEAD + DefaultRecord.recordSizeUpperBound(key, value, headers);
    }

    /**
     * 获取最后一条record的序列号
     * @param sequence
     * @param increment
     * @return
     */
    public static int incrementSequence(int sequence, int increment) {
        // 检查是否出错
        if (sequence > Integer.MAX_VALUE - increment) {
            return increment - (Integer.MAX_VALUE - sequence) - 1;
        }
        // 直接加
        return sequence + increment;
    }

    /**
     * 序列号减少decrement
     * @param sequence
     * @param decrement
     * @return
     */
    public static int decrementSequence(int sequence, int decrement) {
        if (sequence < decrement) {
            return Integer.MAX_VALUE - (decrement - sequence) + 1;
        }
        return sequence - decrement;
    }

    /**
     * record迭代器
     */
    private abstract class RecordIterator implements CloseableIterator<Record> {
        private final Long logAppendTime;
        private final long baseOffset;
        private final long firstTimestamp;
        private final int baseSequence;
        private final int numRecords;
        private int readRecords = 0;

        RecordIterator() {
            this.logAppendTime = timestampType() == TimestampType.LOG_APPEND_TIME ? maxTimestamp() : null;
            this.baseOffset = baseOffset();
            this.firstTimestamp = firstTimestamp();
            this.baseSequence = baseSequence();
            int numRecords = count();
            if (numRecords < 0) {
                throw new InvalidRecordException("Found invalid record count " + numRecords + " in magic v" +
                        magic() + " batch");
            }
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        @Override
        public Record next() {
            if (readRecords >= numRecords) {
                throw new NoSuchElementException();
            }

            readRecords++;
            // 获取下个record
            Record rec = readNext(baseOffset, firstTimestamp, baseSequence, logAppendTime);
            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining()) {
                    throw new InvalidRecordException("Incorrect declared batch size, records still remaining in file");
                }
            }
            return rec;
        }

        protected abstract Record readNext(long baseOffset, long firstTimestamp, int baseSequence, Long logAppendTime);

        protected abstract boolean ensureNoneRemaining();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * 流式Record迭代器
     */
    private abstract class StreamRecordIterator extends RecordIterator {
        private final DataInputStream inputStream;

        StreamRecordIterator(DataInputStream inputStream) {
            super();
            this.inputStream = inputStream;
        }

        abstract Record doReadRecord(long baseOffset, long firstTimestamp, int baseSequence, Long logAppendTime) throws IOException;

        @Override
        protected Record readNext(long baseOffset, long firstTimestamp, int baseSequence, Long logAppendTime) {
            try {
                return doReadRecord(baseOffset, firstTimestamp, baseSequence, logAppendTime);
            } catch (EOFException e) {
                throw new InvalidRecordException("Incorrect declared batch size, premature EOF reached");
            } catch (IOException e) {
                throw new KafkaException("Failed to decompress record stream", e);
            }
        }

        @Override
        protected boolean ensureNoneRemaining() {
            try {
                return inputStream.read() == -1;
            } catch (IOException e) {
                throw new KafkaException("Error checking for remaining bytes after reading batch", e);
            }
        }

        @Override
        public void close() {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new KafkaException("Failed to close record stream", e);
            }
        }
    }

    /**
     * 读取文件的record batch
     */
    static class DefaultFileChannelRecordBatch extends FileLogInputStream.FileChannelRecordBatch {

        DefaultFileChannelRecordBatch(long offset,
                                      byte magic,
                                      FileRecords fileRecords,
                                      int position,
                                      int batchSize) {
            super(offset, magic, fileRecords, position, batchSize);
        }

        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            return new DefaultRecordBatch(buffer);
        }

        @Override
        public long baseOffset() {
            return offset;
        }

        @Override
        public long lastOffset() {
            return loadBatchHeader().lastOffset();
        }

        @Override
        public long producerId() {
            return loadBatchHeader().producerId();
        }

        @Override
        public short producerEpoch() {
            return loadBatchHeader().producerEpoch();
        }

        @Override
        public int baseSequence() {
            return loadBatchHeader().baseSequence();
        }

        @Override
        public int lastSequence() {
            return loadBatchHeader().lastSequence();
        }

        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        @Override
        public Integer countOrNull() {
            return loadBatchHeader().countOrNull();
        }

        @Override
        public boolean isTransactional() {
            return loadBatchHeader().isTransactional();
        }

        @Override
        public boolean isControlBatch() {
            return loadBatchHeader().isControlBatch();
        }

        @Override
        public int partitionLeaderEpoch() {
            return loadBatchHeader().partitionLeaderEpoch();
        }

        @Override
        protected int headerSize() {
            return RECORD_BATCH_OVERHEAD;
        }
    }

}
