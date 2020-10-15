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

/**
 * 用于访问记录的基本接口，这些记录可以包含在日志中，或者日志记录在内存中的具体化。
 * Base interface for accessing records which could be contained in the log, or an in-memory materialization of log records.
 */
public interface BaseRecords {
    /**
     * 这些记录的大小，以字节为单位。
     * The size of these records in bytes.
     * @return The size in bytes of the records
     */
    int sizeInBytes();

    /**
     * 将这个{@link BaseRecords}对象封装到{@link RecordsSend}中
     * Encapsulate this {@link BaseRecords} object into {@link RecordsSend}
     * @param  destination
     * @return Initialized {@link RecordsSend} object
     */
    RecordsSend toSend(String destination);
}
