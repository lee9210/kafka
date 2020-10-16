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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * 用于访问log中包含的batch的抽象实现
 */
public abstract class AbstractRecords implements Records {

    private final Iterable<Record> records = this::recordsIterator;

    /**
     * 检查此缓冲区中的所有batch是否和magic相等。
     * @param magic The magic value to check
     * @return
     */
    @Override
    public boolean hasMatchingMagic(byte magic) {
        for (RecordBatch batch : batches()) {
            if (batch.magic() != magic) {
                return false;
            }
        }
        return true;
    }

    /**
     * 检查该日志缓冲区是否有一个与特定值兼容的magic值(即，该缓冲区中包含的所有消息集是否具有匹配或更低的magic)。
     * @param magic The magic version to ensure compatibility with
     * @return
     */
    @Override
    public boolean hasCompatibleMagic(byte magic) {
        for (RecordBatch batch : batches()) {
            if (batch.magic() > magic) {
                return false;
            }
        }
        return true;
    }

    /**
     * 返回第一条RecordBatch
     * @return
     */
    public RecordBatch firstBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    /**
     * 返回迭代器
     *
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    @Override
    public Iterable<Record> records() {
        return records;
    }

    /**
     * 将数据封装到RecordsSend中
     * @param  destination
     * @return
     */
    @Override
    public RecordsSend toSend(String destination) {
        return new DefaultRecordsSend(destination, this);
    }

    /**
     * 返回Record迭代器
     * @return
     */
    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            /**
             * 获取下一条Record
             * @return
             */
            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext()) {
                    return records.next();
                }
                // 如果还有其他批次，迭代处理
                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }
                // 返回空
                return allDone();
            }
        };
    }

    /**
     * 估计bytes的大小
     * @param magic
     * @param baseOffset
     * @param compressionType
     * @param records
     * @return
     */
    public static int estimateSizeInBytes(byte magic, long baseOffset, CompressionType compressionType, Iterable<Record> records) {
        int size = 0;
        // 根据版本不同，采用不同方式
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records) {
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
            }
        } else {
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    /**
     * 获取records的长度
     * @return
     */
    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<SimpleRecord> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            // 遍历每条record，累加长度
            for (SimpleRecord record : records) {
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
            }
        } else {
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    /**
     * 获取保存具有给定字段的record所需的批大小的估计上限。
     * 这只是一个估计，因为它没有考虑到压缩算法的开销。
     *
     * Get an upper bound estimate on the batch size needed to hold a record with the given fields. This is only
     * an estimate because it does not take into account overhead from the compression algorithm.
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, byte[] key, byte[] value, Header[] headers) {
        return estimateSizeInBytesUpperBound(magic, compressionType, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    /**
     * 获取保存具有给定字段的record所需的batch大小的估计上限。
     * 这只是一个估计，因为它没有考虑到压缩算法的开销。
     *
     * Get an upper bound estimate on the batch size needed to hold a record with the given fields. This is only
     * an estimate because it does not take into account overhead from the compression algorithm.
     */
    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, ByteBuffer key,
                                                    ByteBuffer value, Header[] headers) {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            return DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers);
        } else if (compressionType != CompressionType.NONE) {
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic) + LegacyRecord.recordSize(magic, key, value);
        } else {
            return Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        }
    }

    /**
     * 返回record batch的header
     * Return the size of the record batch header.
     *
     * For V0 and V1 with no compression, it's unclear if Records.LOG_OVERHEAD or 0 should be chosen. There is no header
     * per batch, but a sequence of batches is preceded by the offset and size. This method returns `0` as it's what
     * `MemoryRecordsBuilder` requires.
     */
    public static int recordBatchHeaderSizeInBytes(byte magic, CompressionType compressionType) {
        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        } else if (compressionType != CompressionType.NONE) {
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic);
        } else {
            return 0;
        }
    }


}
