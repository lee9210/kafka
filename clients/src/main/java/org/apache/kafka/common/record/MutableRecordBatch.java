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

import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;

/**
 * 可变record batch可以在适当的地方进行修改(不需要复制)。broker使用它在将batch中的某些字段附加到日志之前覆盖它。
 * A mutable record batch is one that can be modified in place (without copying). This is used by the broker
 * to override certain fields in the batch before appending it to the log.
 */
public interface MutableRecordBatch extends RecordBatch {

    /**
     * 设置此batch的最后偏移量。
     * Set the last offset of this batch.
     * @param offset The last offset to use
     */
    void setLastOffset(long offset);

    /**
     * 设置此batch的最大时间戳
     *
     * Set the max timestamp for this batch. When using log append time, this effectively overrides the individual
     * timestamps of all the records contained in the batch. To avoid recompression, the record fields are not updated
     * by this method, but clients ignore them if the timestamp time is log append time. Note that firstTimestamp is not
     * updated by this method.
     *
     * This typically requires re-computation of the batch's CRC.
     *
     * @param timestampType The timestamp type
     * @param maxTimestamp The maximum timestamp
     */
    void setMaxTimestamp(TimestampType timestampType, long maxTimestamp);

    /**
     * 为这个batch设置partition leader版本号
     * Set the partition leader epoch for this batch of records.
     * @param epoch The partition leader epoch to use
     */
    void setPartitionLeaderEpoch(int epoch);

    /**
     * 将此record batch写入输出流。
     * Write this record batch into an output stream.
     * @param outputStream The buffer to write the batch to
     */
    void writeTo(ByteBufferOutputStream outputStream);

    /**
     * 返回一个迭代器，它跳过解析记录流中的key、value和header，因此得到的{@code org.apache.kafka.common.record.Record}的键和值字段将为空。
     * 这个迭代器在不需要读取记录的键和值时使用，因此可以节省一些字节缓冲区分配/ GC开销。
     *
     * Return an iterator which skips parsing key, value and headers from the record stream, and therefore the resulted
     * {@code org.apache.kafka.common.record.Record}'s key and value fields would be empty. This iterator is used
     * when the read record's key and value are not needed and hence can save some byte buffer allocating / GC overhead.
     *
     * @return The closeable iterator
     */
    CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier);
}
