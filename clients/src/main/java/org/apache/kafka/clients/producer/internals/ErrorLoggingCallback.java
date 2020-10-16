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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 发送batch发生错误的情况下的回调
 */
public class ErrorLoggingCallback implements Callback {
    private static final Logger log = LoggerFactory.getLogger(ErrorLoggingCallback.class);
    private String topic;
    private byte[] key;
    private byte[] value;
    private int valueLength;
    private boolean logAsString;

    public ErrorLoggingCallback(String topic, byte[] key, byte[] value, boolean logAsString) {
        this.topic = topic;
        this.key = key;

        if (logAsString) {
            this.value = value;
        }

        this.valueLength = value == null ? -1 : value.length;
        this.logAsString = logAsString;
    }

    /**
     * 完成之后的动作
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
     *                 with -1 value for all fields except for topicPartition will be returned if an error occurred.
     * @param e
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            String keyString = (key == null) ? "null" :
                    logAsString ? new String(key, StandardCharsets.UTF_8) : key.length + " bytes";
            String valueString = (valueLength == -1) ? "null" :
                    logAsString ? new String(value, StandardCharsets.UTF_8) : valueLength + " bytes";
            log.error("Error when sending message to topic {} with key: {}, value: {} with error:",
                    topic, keyString, valueString, e);
        }
    }
}
