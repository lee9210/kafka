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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于缓存字节缓冲区的简单非线程安全接口。
 * 这适用于一些简单的情况，
 * 比如确保给定的KafkaConsumer在迭代获取的记录时重用相同的解压缩缓冲区。
 * 对于小的记录批，分配一个潜在的大缓冲区(LZ4为64 KB)将控制batch中record的解压缩和迭代的成本。
 *
 * Simple non-threadsafe interface for caching byte buffers. This is suitable for simple cases like ensuring that
 * a given KafkaConsumer reuses the same decompression buffer when iterating over fetched records. For small record
 * batches, allocating a potentially large buffer (64 KB for LZ4) will dominate the cost of decompressing and
 * iterating over the records in the batch.
 */
public abstract class BufferSupplier implements AutoCloseable {

    public static final BufferSupplier NO_CACHING = new BufferSupplier() {
        @Override
        public ByteBuffer get(int capacity) {
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {}

        @Override
        public void close() {}
    };

    public static BufferSupplier create() {
        return new DefaultSupplier();
    }

    /**
     * 提供一个具有所需容量的缓冲区。这可能会返回一个缓存缓冲区或分配一个新实例。
     *
     * Supply a buffer with the required capacity. This may return a cached buffer or allocate a new instance.
     */
    public abstract ByteBuffer get(int capacity);

    /**
     * 返回所提供的缓冲区，以便后续调用' get '时重用。
     *
     * Return the provided buffer to be reused by a subsequent call to `get`.
     */
    public abstract void release(ByteBuffer buffer);

    /**
     * 释放与supplier相关的所有资源。
     *
     * Release all resources associated with this supplier.
     */
    @Override
    public abstract void close();

    /**
     * BufferSupplier的默认实现
     */
    private static class DefaultSupplier extends BufferSupplier {
        // We currently use a single block size, so optimise for that case
        private final Map<Integer, Deque<ByteBuffer>> bufferMap = new HashMap<>(1);

        @Override
        public ByteBuffer get(int size) {
            Deque<ByteBuffer> bufferQueue = bufferMap.get(size);
            if (bufferQueue == null || bufferQueue.isEmpty()) {
                return ByteBuffer.allocate(size);
            } else {
                return bufferQueue.pollFirst();
            }
        }

        @Override
        public void release(ByteBuffer buffer) {
            buffer.clear();
            Deque<ByteBuffer> bufferQueue = bufferMap.get(buffer.capacity());
            if (bufferQueue == null) {
                // We currently keep a single buffer in flight, so optimise for that case
                bufferQueue = new ArrayDeque<>(1);
                bufferMap.put(buffer.capacity(), bufferQueue);
            }
            bufferQueue.addLast(buffer);
        }

        @Override
        public void close() {
            bufferMap.clear();
        }
    }

    /**
     * 简单的缓冲区供应单线程使用。它缓存一个单一的缓冲区，该缓冲区根据需要单调增长，以满足分配请求。
     *
     * Simple buffer supplier for single-threaded usage. It caches a single buffer, which grows
     * monotonically as needed to fulfill the allocation request.
     */
    public static class GrowableBufferSupplier extends BufferSupplier {
        private ByteBuffer cachedBuffer;

        @Override
        public ByteBuffer get(int minCapacity) {
            if (cachedBuffer != null && cachedBuffer.capacity() >= minCapacity) {
                ByteBuffer res = cachedBuffer;
                cachedBuffer = null;
                return res;
            } else {
                cachedBuffer = null;
                return ByteBuffer.allocate(minCapacity);
            }
        }

        @Override
        public void release(ByteBuffer buffer) {
            buffer.clear();
            cachedBuffer = buffer;
        }

        @Override
        public void close() {
            cachedBuffer = null;
        }
    }

}
