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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;


/**
 * 保存在给定内存限制下的字节缓冲区池。
 * 1. 有一个特殊的“可池大小”，这种大小的缓冲区保存在一个空闲列表中并回收
 * 2. 这是公平的。也就是说，所有的内存都给了等待时间最长的线程，直到它有足够的内存。这可以防止线程在请求大内存块并需要阻塞直到多个缓冲区被释放时出现饥饿或死锁。
 *
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";
    /** 整个BufferPool的大小 */
    private final long totalMemory;
    /** 一次批处理的大小，一般是batch.size的大小 */
    private final int poolableSize;
    /** 线程控制锁 */
    private final ReentrantLock lock;
    /** 空闲ByteBuffer队列 */
    private final Deque<ByteBuffer> free;
    /** 阻塞的线程,等待申请空间 */
    private final Deque<Condition> waiters;
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */
    // 申请分配内存过程中，检测到有足够内存可供分配，在逐渐从free中获取buffer的过程中，获取的空间的累加的数据总和
    // pool的可用空间总和应该是free * poolableSize + nonPooledAvailableMemory
    private long nonPooledAvailableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
    private boolean closed;

    /**
     * 创建一个buffer pool
     * Create a new buffer pool
     *
     * @param memory 此缓冲池可分配的最大内存量 The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize 要在空闲列表中缓存而不是释放的缓冲区大小 The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        // 创建监控数据
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total",
                                                   metricGrpName,
                                                   "The total time an appender waits for space allocation.");

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
        // 标记可用
        this.closed = false;
    }

    /**
     * 分配一个给定大小的缓冲区。如果没有足够的内存并且缓冲池配置为阻塞模式，则此方法将阻塞。
     *
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size 需要分配的空间。 The buffer size to allocate in bytes
     * @param maxTimeToBlockMs 最长的阻塞时间。The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        // 如果需要的size大于本BufferPool的总大小，抛出异常。
        if (size > this.totalMemory) {
            throw new IllegalArgumentException("Attempt to allocate " + size + " bytes, but there is a hard limit of " + this.totalMemory + " on memory allocations.");
        }

        ByteBuffer buffer = null;
        // 加锁
        this.lock.lock();
        // 如果已经关闭，则放开锁，并且抛出异常。
        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        try {
            // 如果申请的大小和poolableSize相等，并且剩余的待分配队列不为空，则直接取出一个返回
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty()) {
                return this.free.pollFirst();
            }

            // now check if the request is immediately satisfiable with the memory on hand or if we need to block
            // 现在检查当前的内存是否可以立即满足请求，或者是否需要阻塞
            // 计算pool剩余的未分配buffer大小
            int freeListSize = freeSize() * this.poolableSize;
            // 够
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately satisfy the request, but need to allocate the buffer
                // 我们有足够的未分配或池内存来立即满足请求，但需要分配缓冲区
                freeUp(size);
                // 已释放的空间减去需要的size，保证数据结构一致
                this.nonPooledAvailableMemory -= size;
            // 不够
            } else {
                // we are out of memory and will have to block
                int accumulated = 0;
                Condition moreMemory = this.lock.newCondition();
                try {
                    // 剩余的阻塞时间
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    // 将激活条件加入到waiters尾部
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            // 等待remainingTimeToBlockNs长的时间
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            // 记录等待时间
                            recordWaitTime(timeNs);
                        }
                        // 如果过程中关闭了，则抛出异常
                        if (this.closed) {
                            throw new KafkaException("Producer closed while allocating memory");
                        }

                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }
                        // 减去等待过的时间
                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list, otherwise allocate memory
                        // 如果未分配，并且需要的size等于free中的一个，并且free不为空，则获取一个
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            // 获取一个buffer
                            buffer = this.free.pollFirst();
                            accumulated = size;

                        } else {
                            // we'll need to allocate memory, but we may only get part of what we need on this iteration
                            // 从free中释放空间
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            // 计算本次迭代获取的空间
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    // 本地迭代置0
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    // 把获取到的数据返回到nonPooledAvailableMemory中
                    this.nonPooledAvailableMemory += accumulated;
                    // 移除
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                // 如果有剩余空间，则唤醒。
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty()) {
                    this.waiters.peekFirst().signal();
                }
            } finally {
                // Another finally... otherwise find bugs complains
                // 释放锁
                lock.unlock();
            }
        }
        // 返回获取到的buffer
        if (buffer != null) {
            return buffer;
        } else {
            // 从内存中分配size空间，并加入到pool中管理
            return safeAllocateByteBuffer(size);
        }
    }

    /**
     * 记录时间
     * Protected for testing
     */
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * 分配一个缓冲区。
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            if (error) {
                this.lock.lock();
                try {
                    // 把分配的空间加到nonPooledAvailableMemory中
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty()) {
                        // 激活第一个
                        this.waiters.peekFirst().signal();
                    }
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    /**
     * 直接分配size大小的空间
     * @param size 大小
     * @return
     */
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * 从free中释放空间，释放的空间累加到nonPooledAvailableMemory中
     * 通过释放池缓冲(如果需要)，尝试确保我们至少有请求的内存字节数可供分配
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size) {
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
        }
    }

    /**
     * 向pool中返回缓冲区，
     * 如果有poolable大小，则将其添加到空闲列表中，否则仅将内存标记为空闲。
     *
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer 返回的buffer。 The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     *             buffer的大小，可能比buffer.capacity小，因为缓冲区可能有压缩。
     */
    public void deallocate(ByteBuffer buffer, int size) {
        // 加锁
        lock.lock();
        try {
            // 如果等于poolableSize，则直接加到free中
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            // 否则加入到nonPooledAvailableMemory中
            } else {
                this.nonPooledAvailableMemory += size;
            }
            // 唤醒一个waiter
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null) {
                // 唤醒获取线程
                moreMem.signal();
            }
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 释放内存到pool中
     * @param buffer
     */
    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * 返回pool可用空间
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * 获取nonPooledAvailableMemory空间
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取等待分配空间线程数
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * 关闭pool，内存不能继续分配，已经分配的内存可以释放。等待分配内存的线程都将失败。
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     */
    public void close() {
        this.lock.lock();
        this.closed = true;
        try {
            // 唤醒所有waiter
            for (Condition waiter : this.waiters) {
                waiter.signal();
            }
        } finally {
            this.lock.unlock();
        }
    }
}
