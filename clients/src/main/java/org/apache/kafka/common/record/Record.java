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

import org.apache.kafka.common.header.Header;

/**
 * 一条日志record。
 * 日志record是一个元组，由日志中的唯一偏移量、生产者分配的序列号、时间戳、键和值组成。
 *
 * A log record is a tuple consisting of a unique offset in the log, a sequence number assigned by
 * the producer, a timestamp, a key and a value.
 */
public interface Record {

    Header[] EMPTY_HEADERS = new Header[0];

    /**
     * 这条消息的offset
     * The offset of this record in the log
     * @return the offset
     */
    long offset();

    /**
     * 获取生产者分配的序列号。
     * Get the sequence number assigned by the producer.
     * @return the sequence number
     */
    int sequence();

    /**
     * 获取该record的大小(以字节为单位)。
     * Get the size in bytes of this record.
     * @return the size of the record in bytes
     */
    int sizeInBytes();

    /**
     * 获取record的时间戳。
     * Get the record's timestamp.
     * @return the record's timestamp
     */
    long timestamp();

    /**
     * 获取record内容的checksum。
     * Get a checksum of the record contents.
     * @return A 4-byte unsigned checksum represented as a long or null if the message format does not
     *         include a checksum (i.e. for v2 and above)
     */
    Long checksumOrNull();

    /**
     * 检查record是否具有有效的checksum。
     * Check whether the record has a valid checksum.
     * @return true if the record has a valid checksum, false otherwise
     */
    boolean isValid();

    /**
     * 如果没有checksum,则抛出{@link org.apache.kafka.common.errors.CorruptRecordException}异常
     * Raise a {@link org.apache.kafka.common.errors.CorruptRecordException} if the record does not have a valid checksum.
     */
    void ensureValid();

    /**
     * 获取key的大小(以字节为单位)。
     * Get the size in bytes of the key.
     * @return the size of the key, or -1 if there is no key
     */
    int keySize();

    /**
     * 检查record是否有key
     * Check whether this record has a key
     * @return true if there is a key, false otherwise
     */
    boolean hasKey();

    /**
     * 获取key的ByteBuffer
     * Get the record's key.
     * @return the key or null if there is none
     */
    ByteBuffer key();

    /**
     * 获取value的大小（以字节为单位）
     * Get the size in bytes of the value.
     * @return the size of the value, or -1 if the value is null
     */
    int valueSize();

    /**
     * 检查是否有value
     * Check whether a value is present (i.e. if the value is not null)
     * @return true if so, false otherwise
     */
    boolean hasValue();

    /**
     * 获取value的ByteBuffer
     * Get the record's value
     * @return the (nullable) value
     */
    ByteBuffer value();

    /**
     * 检查record是否有特殊的magic。对于2之前的版本，record包含它自己的magic，所以这个函数可以用来检查它是否匹配特定的值。
     * 对于版本2及以上版本，如果传递的magic大于或等于2，则此方法返回true。
     *
     * Check whether the record has a particular magic. For versions prior to 2, the record contains its own magic,
     * so this function can be used to check whether it matches a particular value. For version 2 and above, this
     * method returns true if the passed magic is greater than or equal to 2.
     *
     * @param magic the magic value to check
     * @return true if the record has a magic field (versions prior to 2) and the value matches
     */
    boolean hasMagic(byte magic);

    /**
     * 对于2之前的版本，检查record是否被压缩(因此有嵌套的record内容)。
     * 对于版本2和更高版本，这总是返回false。
     *
     * For versions prior to 2, check whether the record is compressed (and therefore
     * has nested record content). For versions 2 and above, this always returns false.
     * @return true if the magic is lower than 2 and the record is compressed
     */
    boolean isCompressed();

    /**
     * 此方法可用于检查该属性的值是否与特定的时间戳类型匹配。
     * 对于2之前的版本，record包含一个时间戳类型属性。
     * 对于版本2及以上，这将始终为false。
     *
     * For versions prior to 2, the record contained a timestamp type attribute. This method can be
     * used to check whether the value of that attribute matches a particular timestamp type. For versions
     * 2 and above, this will always be false.
     *
     * @param timestampType the timestamp type to compare
     * @return true if the version is lower than 2 and the timestamp type matches
     */
    boolean hasTimestampType(TimestampType timestampType);

    /**
     * 获取headers。
     * 对于magic版本1及以下，它总是返回一个空数组。
     * Get the headers. For magic versions 1 and below, this always returns an empty array.
     *
     * @return the array of headers
     */
    Header[] headers();
}
