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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Checksums;
import org.apache.kafka.common.utils.Crc32;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * 这个类表示序列化的键和值以及相关的CRC和消息格式版本0和1的其他字段。
 * 注意，不太需要直接访问这个类。通常应该通过通过{@link Records}对象公开的{@link Record}接口间接访问它。
 *
 * This class represents the serialized key and value along with the associated CRC and other fields
 * of message format versions 0 and 1. Note that it is uncommon to need to access this class directly.
 * Usually it should be accessed indirectly through the {@link Record} interface which is exposed
 * through the {@link Records} object.
 */
public final class LegacyRecord {

    /**
     * 当前消息字段的顺序和长度
     * The current offset and size for all the fixed-length fields
     */
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTES_LENGTH = 1;
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * 不同版本header的长度
     * The size for the record header
     */
    public static final int HEADER_SIZE_V0 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;
    public static final int HEADER_SIZE_V1 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH;

    /**
     * overhead的长度
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;
    public static final int RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;


    /**
     * 指定压缩代码的掩码。3位保存压缩编解码器。保留0表示没有压缩
     * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    private static final int COMPRESSION_CODEC_MASK = 0x07;

    /**
     * 指定时间戳类型的掩码:0表示CreateTime, 1表示LogAppendTime。
     * Specify the mask of timestamp type: 0 for CreateTime, 1 for LogAppendTime.
     */
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    /**
     * 没有时间戳的记录的时间戳值
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public LegacyRecord(ByteBuffer buffer) {
        this(buffer, null, null);
    }

    public LegacyRecord(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    /**
     * 根据record内容计算record的校验码
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return Crc32.crc32(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * 检索此记录以前计算的CRC
     * Retrieve the previously computed CRC for this record
     */
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * 如果存储在记录中的crc与从记录内容中计算出的crc匹配，则返回true
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     */
    public boolean isValid() {
        return sizeInBytes() >= RECORD_OVERHEAD_V0 && checksum() == computeChecksum();
    }

    public Long wrapperRecordTimestamp() {
        return wrapperRecordTimestamp;
    }

    public TimestampType wrapperRecordTimestampType() {
        return wrapperRecordTimestampType;
    }

    /**
     * 如果该记录的isValid为false，则抛出InvalidRecordException
     * Throw an InvalidRecordException if isValid is false for this record
     */
    public void ensureValid() {
        if (sizeInBytes() < RECORD_OVERHEAD_V0) {
            throw new CorruptRecordException("Record is corrupt (crc could not be retrieved as the record is too "
                    + "small, size = " + sizeInBytes() + ")");
        }

        if (!isValid()) {
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
        }
    }

    /**
     * 该记录的完整序列化大小(以字节为单位)(包括crc、头属性等)，但不包括日志overhead(偏移量和记录大小)。
     * The complete serialized size of this record in bytes (including crc, header attributes, etc), but
     * excluding the log overhead (offset and record size).
     * @return the size in bytes
     */
    public int sizeInBytes() {
        return buffer.limit();
    }

    /**
     * 获取key的长度
     * The length of the key in bytes
     * @return the size in bytes of the key (0 if the key is null)
     */
    public int keySize() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0) {
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        } else {
            return buffer.getInt(KEY_SIZE_OFFSET_V1);
        }
    }

    /**
     * 判断有key
     * Does the record have a key?
     * @return true if so, false otherwise
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * 返回value的位置
     * The position where the value size is stored
     */
    private int valueSizeOffset() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0) {
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        } else {
            return KEY_OFFSET_V1 + Math.max(0, keySize());
        }
    }

    /**
     * value的长度
     * The length of the value in bytes
     * @return the size in bytes of the value (0 if the value is null)
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * 检查此记录的值字段是否为空。
     * Check whether the value field of this record is null.
     * @return true if the value is null, false otherwise
     */
    public boolean hasNullValue() {
        return valueSize() < 0;
    }

    /**
     * magic的值
     * The magic value (i.e. message format version) of this record
     * @return the magic value
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * 获取magic
     * The attributes stored with this record
     * @return the attributes
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * 当magic值大于0时，记录的时间戳按以下方式确定:
     * 1. wrapperRecordTimestampType = null和wrapperRecordTimestamp(空)-未压缩的消息，时间戳在消息中。
     * 2. wrapperRecordTimestampType = LOG_APPEND_TIME和WrapperRecordTimestamp(不是空):压缩消息使用LOG_APPEND_TIME
     * 3.wrapperRecordTimestampType = CREATE_TIME和wrapperRecordTimestamp(不是空):压缩消息使用CREATE_TIME
     *
     * When magic value is greater than 0, the timestamp of a record is determined in the following way:
     * 1. wrapperRecordTimestampType = null and wrapperRecordTimestamp is null - Uncompressed message, timestamp is in the message.
     * 2. wrapperRecordTimestampType = LOG_APPEND_TIME and WrapperRecordTimestamp is not null - Compressed message using LOG_APPEND_TIME
     * 3. wrapperRecordTimestampType = CREATE_TIME and wrapperRecordTimestamp is not null - Compressed message using CREATE_TIME
     *
     * @return the timestamp as determined above
     */
    public long timestamp() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0) {
            return RecordBatch.NO_TIMESTAMP;
        } else {
            // case 2
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null) {
                return wrapperRecordTimestamp;
            }
            // Case 1, 3
            else {
                return buffer.getLong(TIMESTAMP_OFFSET);
            }
        }
    }

    /**
     * 获取record的TimestampType
     * Get the timestamp type of the record.
     *
     * @return The timestamp type or {@link TimestampType#NO_TIMESTAMP_TYPE} if the magic is 0.
     */
    public TimestampType timestampType() {
        return timestampType(magic(), wrapperRecordTimestampType, attributes());
    }

    /**
     * 获取record的CompressionType(压缩方式)
     * The compression type used with this record
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * 获取value的ByteBuffer
     * A ByteBuffer containing the value of this record
     * @return the value or null if the value for this record is null
     */
    public ByteBuffer value() {
        return Utils.sizeDelimited(buffer, valueSizeOffset());
    }

    /**
     * 获取key的波耶特ByteBuffer
     * A ByteBuffer containing the message key
     * @return the buffer or null if the key for this record is null
     */
    public ByteBuffer key() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0) {
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V0);
        } else {
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V1);
        }
    }

    /**
     * 获取此record实例的底层缓冲区。
     * Get the underlying buffer backing this record instance.
     * @return the buffer
     */
    public ByteBuffer buffer() {
        return this.buffer;
    }

    @Override
    public String toString() {
        if (magic() > 0) {
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, %s=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 timestampType(),
                                 timestamp(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
        } else {
            return String.format("Record(magic=%d, attributes=%d, compression=%s, crc=%d, key=%d bytes, value=%d bytes)",
                                 magic(),
                                 attributes(),
                                 compressionType(),
                                 checksum(),
                                 key() == null ? 0 : key().limit(),
                                 value() == null ? 0 : value().limit());
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!other.getClass().equals(LegacyRecord.class)) {
            return false;
        }
        LegacyRecord record = (LegacyRecord) other;
        return this.buffer.equals(record.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    /**
     * 创建一个新的record实例。如果record的压缩类型不是none，那么它的值有效负载应该已经被指定的类型压缩;
     * 构造函数将始终按原样写入值有效负载，而不会进行压缩本身。
     * Create a new record instance. If the record's compression type is not none, then
     * its value payload should be already compressed with the specified type; the constructor
     * would always write the value payload as is and will not do the compression itself.
     *
     * @param magic The magic value to use
     * @param timestamp The timestamp of the record
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param compressionType The compression type used on the contents of the record (if any)
     * @param timestampType The timestamp type to be used for this record
     */
    public static LegacyRecord create(byte magic,
                                      long timestamp,
                                      byte[] key,
                                      byte[] value,
                                      CompressionType compressionType,
                                      TimestampType timestampType) {
        int keySize = key == null ? 0 : key.length;
        int valueSize = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize));
        write(buffer, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
        buffer.rewind();
        return new LegacyRecord(buffer);
    }

    /**
     * 根据默认方式创建一个LegacyRecord对象
     */
    public static LegacyRecord create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    /**
     * 就地写入压缩记录集的头(例如，假设压缩记录数据已经按照包装记录的值偏移量写入)。
     * 这使您可以动态创建压缩消息集，然后稍后返回并填写其大小和CRC，这样就不需要复制到另一个缓冲区。
     *
     * Write the header for a compressed record set in-place (i.e. assuming the compressed record data has already
     * been written at the value offset in a wrapped record). This lets you dynamically create a compressed message
     * set, and then go back later and fill in its size and CRC, which saves the need for copying to another buffer.
     *
     * @param buffer The buffer containing the compressed record data positioned at the first offset of the
     * @param magic The magic value of the record set
     * @param recordSize The size of the record (including record overhead)
     * @param timestamp The timestamp of the wrapper record
     * @param compressionType The compression type used
     * @param timestampType The timestamp type of the wrapper record
     */
    public static void writeCompressedRecordHeader(ByteBuffer buffer,
                                                   byte magic,
                                                   int recordSize,
                                                   long timestamp,
                                                   CompressionType compressionType,
                                                   TimestampType timestampType) {
        // buffer有值的长度
        int recordPosition = buffer.position();
        // value长度
        int valueSize = recordSize - recordOverhead(magic);

        // write the record header with a null value (the key is always null for the wrapper)
        // 使用空值写入record header(包装器的key始终为空)
        write(buffer, magic, timestamp, null, null, compressionType, timestampType);
        // 设置buffer的位置为recordPosition
        buffer.position(recordPosition);

        // now fill in the value size
        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        // compute and fill the crc from the beginning of the message
        long crc = Crc32.crc32(buffer, MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    /**
     * 往buffer中写数据
     */
    private static void write(ByteBuffer buffer,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) {
        try {
            DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
            write(out, magic, timestamp, key, value, compressionType, timestampType);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * 使用给定的压缩类型写入记录数据，并返回计算的crc。
     * Write the record data with the given compression type and return the computed crc.
     *
     * @param out The output stream to write to
     * @param magic The magic value to be used
     * @param timestamp The timestamp of the record
     * @param key The record key
     * @param value The record value
     * @param compressionType The compression type
     * @param timestampType The timestamp type
     * @return the computed CRC for this record.
     * @throws IOException for any IO errors writing to the output stream.
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             byte[] key,
                             byte[] value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        return write(out, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
    }

    /**
     * 使用给定的压缩类型写入记录数据，并返回计算的crc。
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             ByteBuffer key,
                             ByteBuffer value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        byte attributes = computeAttributes(magic, compressionType, timestampType);
        long crc = computeChecksum(magic, attributes, timestamp, key, value);
        write(out, magic, crc, attributes, timestamp, key, value);
        return crc;
    }

    /**
     * 使用原始字段(不进行验证)编写记录。这只能在测试中使用。
     * Write a record using raw fields (without validation). This should only be used in testing.
     */
    public static void write(DataOutputStream out,
                             byte magic,
                             long crc,
                             byte attributes,
                             long timestamp,
                             byte[] key,
                             byte[] value) throws IOException {
        write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    /**
     * 向缓冲区写入一条记录，如果记录的压缩类型为none，那么它的值负载应该已经用指定的类型进行了压缩
     * Write a record to the buffer, if the record's compression type is none, then
     * its value payload should be already compressed with the specified type
     * @throws IOException
     */
    private static void write(DataOutputStream out,
                              byte magic,
                              long crc,
                              byte attributes,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value) throws IOException {
        if (magic != RecordBatch.MAGIC_VALUE_V0 && magic != RecordBatch.MAGIC_VALUE_V1) {
            throw new IllegalArgumentException("Invalid magic value " + magic);
        }
        if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP) {
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);
        }

        // write crc
        out.writeInt((int) (crc & 0xffffffffL));
        // write magic value
        out.writeByte(magic);
        // write attributes
        out.writeByte(attributes);

        // maybe write timestamp
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            out.writeLong(timestamp);
        }

        // write the key
        if (key == null) {
            out.writeInt(-1);
        } else {
            int size = key.remaining();
            out.writeInt(size);
            Utils.writeTo(out, key, size);
        }
        // write the value
        if (value == null) {
            out.writeInt(-1);
        } else {
            int size = value.remaining();
            out.writeInt(size);
            Utils.writeTo(out, value, size);
        }
    }

    /**
     * 获取record长度
     * @return
     */
    static int recordSize(byte magic, ByteBuffer key, ByteBuffer value) {
        return recordSize(magic, key == null ? 0 : key.limit(), value == null ? 0 : value.limit());
    }

    /**
     * 获取record长度
     * @return
     */
    public static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    /**
     * 获取Attributes byte
     * @return
     */
    // visible only for testing
    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        byte attributes = 0;
        if (type.id > 0) {
            attributes |= COMPRESSION_CODEC_MASK & type.id;
        }
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE) {
                throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for " +
                        "message format v1");
            }
            if (timestampType == TimestampType.LOG_APPEND_TIME) {
                attributes |= TIMESTAMP_TYPE_MASK;
            }
        }
        return attributes;
    }


    /**
     * 获取验证码长度
     * @return
     */
    // visible only for testing
    public static long computeChecksum(byte magic, byte attributes, long timestamp, byte[] key, byte[] value) {
        return computeChecksum(magic, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    /**
     * 根据属性、键和值有效负载计算记录的校验和
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    private static long computeChecksum(byte magic, byte attributes, long timestamp, ByteBuffer key, ByteBuffer value) {
        Crc32 crc = new Crc32();
        crc.update(magic);
        crc.update(attributes);
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            Checksums.updateLong(crc, timestamp);
        }
        // update for the key
        if (key == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = key.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, key, size);
        }
        // update for the value
        if (value == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = value.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, value, size);
        }
        return crc.getValue();
    }

    /**
     * 返回overhead长度
     * @param magic
     * @return
     */
    static int recordOverhead(byte magic) {
        if (magic == 0) {
            return RECORD_OVERHEAD_V0;
        } else if (magic == 1) {
            return RECORD_OVERHEAD_V1;
        }
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    /**
     * 获取header长度的
     * @return
     */
    static int headerSize(byte magic) {
        if (magic == 0) {
            return HEADER_SIZE_V0;
        } else if (magic == 1) {
            return HEADER_SIZE_V1;
        }
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    /**
     * 获取key起始位置
     * @return
     */
    private static int keyOffset(byte magic) {
        if (magic == 0) {
            return KEY_OFFSET_V0;
        } else if (magic == 1) {
            return KEY_OFFSET_V1;
        }
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    /**
     * 获取时间格式
     * @return
     */
    public static TimestampType timestampType(byte magic, TimestampType wrapperRecordTimestampType, byte attributes) {
        if (magic == 0) {
            return TimestampType.NO_TIMESTAMP_TYPE;
        } else if (wrapperRecordTimestampType != null) {
            return wrapperRecordTimestampType;
        } else {
            return (attributes & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
        }
    }

}
