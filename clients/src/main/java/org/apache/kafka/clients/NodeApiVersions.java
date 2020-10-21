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
package org.apache.kafka.clients;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 表示特定节点支持的API版本的内部类。
 *
 * An internal class which represents the API versions supported by a particular node.
 */
public class NodeApiVersions {

    // A map of the usable versions of each API, keyed by the ApiKeys instance
    /** 每个API可用版本的map，由ApiKeys实例进行键控 */
    private final Map<ApiKeys, ApiVersion> supportedVersions = new EnumMap<>(ApiKeys.class);

    // List of APIs which the broker supports, but which are unknown to the client
    /** broker支持但客户端不知道的api列表 */
    private final List<ApiVersion> unknownApis = new ArrayList<>();

    /**
     * 使用当前的api版本创建一个NodeApiVersions对象。
     *
     * Create a NodeApiVersions object with the current ApiVersions.
     *
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create() {
        return create(Collections.emptyList());
    }

    /**
     * 创建一个NodeApiVersions对象。
     * 用传入的API版本加上已经启用的，组成新的合集返回
     * Create a NodeApiVersions object.
     *
     * @param overrides 要覆盖的API版本。此处未指定的任何ApiVersion都将设置为当前客户端值。
     *                  API versions to override. Any ApiVersion not specified here will be set to the current client
     *                  value.
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create(Collection<ApiVersion> overrides) {
        // 转换成list
        List<ApiVersion> apiVersions = new LinkedList<>(overrides);
        // 遍历已经启用的ApiKey
        for (ApiKeys apiKey : ApiKeys.enabledApis()) {
            boolean exists = false;
            for (ApiVersion apiVersion : apiVersions) {
                // 如果相等，则
                if (apiVersion.apiKey == apiKey.id) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                apiVersions.add(new ApiVersion(apiKey));
            }
        }
        return new NodeApiVersions(apiVersions);
    }


    /**
     * Create a NodeApiVersions object with a single ApiKey. It is mainly used in tests.
     *
     * @param apiKey ApiKey's id.
     * @param minVersion ApiKey's minimum version.
     * @param maxVersion ApiKey's maximum version.
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create(short apiKey, short minVersion, short maxVersion) {
        return create(Collections.singleton(new ApiVersion(apiKey, minVersion, maxVersion)));
    }

    /**
     * 初始化NodeApiVersions
     * 本地有的话才加入supportedVersions，本地没有则加入unknownApis
     */
    public NodeApiVersions(ApiVersionsResponseKeyCollection nodeApiVersions) {
        for (ApiVersionsResponseKey nodeApiVersion : nodeApiVersions) {
            if (ApiKeys.hasId(nodeApiVersion.apiKey())) {
                ApiKeys nodeApiKey = ApiKeys.forId(nodeApiVersion.apiKey());
                supportedVersions.put(nodeApiKey, new ApiVersion(nodeApiVersion));
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknownApis.add(new ApiVersion(nodeApiVersion));
            }
        }
    }

    /**
     * 初始化NodeApiVersions
     * 本地有的话才加入supportedVersions，本地没有则加入unknownApis
     */
    public NodeApiVersions(Collection<ApiVersion> nodeApiVersions) {
        for (ApiVersion nodeApiVersion : nodeApiVersions) {
            if (ApiKeys.hasId(nodeApiVersion.apiKey)) {
                ApiKeys nodeApiKey = ApiKeys.forId(nodeApiVersion.apiKey);
                supportedVersions.put(nodeApiKey, nodeApiVersion);
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknownApis.add(nodeApiVersion);
            }
        }
    }

    /**
     * 返回节点和本地支持的最新版本。
     *
     * Return the most recent version supported by both the node and the local software.
     */
    public short latestUsableVersion(ApiKeys apiKey) {
        return latestUsableVersion(apiKey, apiKey.oldestVersion(), apiKey.latestVersion());
    }

    /**
     * 在允许的版本范围内获得broker支持的最新版本
     *
     * Get the latest version supported by the broker within an allowed range of versions
     */
    public short latestUsableVersion(ApiKeys apiKey, short oldestAllowedVersion, short latestAllowedVersion) {
        ApiVersion usableVersion = supportedVersions.get(apiKey);
        if (usableVersion == null) {
            throw new UnsupportedVersionException("The broker does not support " + apiKey);
        }
        return latestUsableVersion(apiKey, usableVersion, oldestAllowedVersion, latestAllowedVersion);
    }

    /**
     * 获取本地支持的最大版本
     */
    private short latestUsableVersion(ApiKeys apiKey, ApiVersion supportedVersions,
                                      short minAllowedVersion, short maxAllowedVersion) {
        short minVersion = (short) Math.max(minAllowedVersion, supportedVersions.minVersion);
        short maxVersion = (short) Math.min(maxAllowedVersion, supportedVersions.maxVersion);
        if (minVersion > maxVersion) {
            throw new UnsupportedVersionException("The broker does not support " + apiKey +
                    " with version in range [" + minAllowedVersion + "," + maxAllowedVersion + "]. The supported" +
                    " range is [" + supportedVersions.minVersion + "," + supportedVersions.maxVersion + "].");
        }
        return maxVersion;
    }

    /**
     * Convert the object to a string with no linebreaks.<p/>
     * <p>
     * This toString method is relatively expensive, so avoid calling it unless debug logging is turned on.
     */
    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * Convert the object to a string.
     *
     * @param lineBreaks True if we should add a linebreak after each api.
     */
    public String toString(boolean lineBreaks) {
        // The apiVersion collection may not be in sorted order.  We put it into
        // a TreeMap before printing it out to ensure that we always print in
        // ascending order.
        TreeMap<Short, String> apiKeysText = new TreeMap<>();
        for (ApiVersion supportedVersion : this.supportedVersions.values()) {
            apiKeysText.put(supportedVersion.apiKey, apiVersionToText(supportedVersion));
        }
        for (ApiVersion apiVersion : unknownApis) {
            apiKeysText.put(apiVersion.apiKey, apiVersionToText(apiVersion));
        }

        // Also handle the case where some apiKey types are not specified at all in the given ApiVersions,
        // which may happen when the remote is too old.
        for (ApiKeys apiKey : ApiKeys.enabledApis()) {
            if (!apiKeysText.containsKey(apiKey.id)) {
                StringBuilder bld = new StringBuilder();
                bld.append(apiKey.name).append("(").
                        append(apiKey.id).append("): ").append("UNSUPPORTED");
                apiKeysText.put(apiKey.id, bld.toString());
            }
        }
        String separator = lineBreaks ? ",\n\t" : ", ";
        StringBuilder bld = new StringBuilder();
        bld.append("(");
        if (lineBreaks) {
            bld.append("\n\t");
        }
        bld.append(Utils.join(apiKeysText.values(), separator));
        if (lineBreaks) {
            bld.append("\n");
        }
        bld.append(")");
        return bld.toString();
    }

    /**
     * 转换成String
     */
    private String apiVersionToText(ApiVersion apiVersion) {
        StringBuilder bld = new StringBuilder();
        ApiKeys apiKey = null;
        if (ApiKeys.hasId(apiVersion.apiKey)) {
            apiKey = ApiKeys.forId(apiVersion.apiKey);
            bld.append(apiKey.name).append("(").append(apiKey.id).append("): ");
        } else {
            bld.append("UNKNOWN(").append(apiVersion.apiKey).append("): ");
        }

        if (apiVersion.minVersion == apiVersion.maxVersion) {
            bld.append(apiVersion.minVersion);
        } else {
            bld.append(apiVersion.minVersion).append(" to ").append(apiVersion.maxVersion);
        }

        if (apiKey != null) {
            ApiVersion supportedVersion = supportedVersions.get(apiKey);
            if (apiKey.latestVersion() < supportedVersion.minVersion) {
                bld.append(" [unusable: node too new]");
            } else if (supportedVersion.maxVersion < apiKey.oldestVersion()) {
                bld.append(" [unusable: node too old]");
            } else {
                short latestUsableVersion = Utils.min(apiKey.latestVersion(), supportedVersion.maxVersion);
                bld.append(" [usable: ").append(latestUsableVersion).append("]");
            }
        }
        return bld.toString();
    }

    /**
     * 根据api获取本地支持的对应的api信息
     * Get the version information for a given API.
     *
     * @param apiKey The api key to lookup
     * @return The api version information from the broker or null if it is unsupported
     */
    public ApiVersion apiVersion(ApiKeys apiKey) {
        return supportedVersions.get(apiKey);
    }

}
