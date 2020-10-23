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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.Closeable;
import java.util.List;

/**
 * 节点管理
 * The interface for {@link NetworkClient}
 */
public interface KafkaClient extends Closeable {

    /**
     * 检查是否已经准备好向给定节点发送另一个请求，但如果没有准备好，就不要尝试连接。
     *
     * Check if we are currently ready to send another request to the given node but don't attempt to connect if we
     * aren't.
     *
     * @param node The node to check
     * @param now The current timestamp
     */
    boolean isReady(Node node, long now);

    /**
     * 启动到给定节点的连接(如果需要)，如果已经连接，则返回true。
     * 只有在调用轮询时，节点的准备状态才会发生变化。
     *
     * Initiate a connection to the given node (if necessary), and return true if already connected. The readiness of a
     * node will change only when poll is invoked.
     *
     * @param node The node to connect to.
     * @param now The current time
     * @return true iff we are ready to immediately initiate the sending of another request to the given node.
     */
    boolean ready(Node node, long now);

    /**
     * 根据连接状态返回尝试发送数据之前等待的毫秒数。
     * 当断开连接时，这将考虑重新连接回退时间。
     * 当连接或连接时，这处理缓慢/停滞的连接。
     *
     * Return the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    long connectionDelay(Node node, long now);

    /**
     * 在尝试发送数据之前，根据连接状态和节流时间返回等待的毫秒数。
     * 如果连接已经建立，但正在节流，则返回节流延迟。否则，返回连接延迟。
     *
     * Return the number of milliseconds to wait, based on the connection state and the throttle time, before
     * attempting to send data. If the connection has been established but being throttled, return throttle delay.
     * Otherwise, return connection delay.
     *
     * @param node the connection to check
     * @param now the current time in ms
     */
    long pollDelayMs(Node node, long now);

    /**
     * 根据连接状态，检查节点的连接是否失败。
     * 这种连接故障通常是暂态的，可以在下一次{@link #ready(org.apache.kafka.common.Node, long)} }调用中恢复，
     * 但在某些情况下，需要捕获暂态故障并对其进行重新处理。
     *
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    boolean connectionFailed(Node node);

    /**
     * 根据连接状态，检查对该节点的身份验证是否失败。
     * 传播身份验证失败而不重试。
     *
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    AuthenticationException authenticationException(Node node);

    /**
     * 将给定的请求排队等待发送。
     * 请求只能在就绪的连接上发送。
     *
     * Queue up the given request for sending. Requests can only be sent on ready connections.
     * @param request The request
     * @param now The current timestamp
     */
    void send(ClientRequest request, long now);

    /**
     * 获取socket的返回结果
     * Do actual reads and writes from sockets.
     *
     * @param timeout The maximum amount of time to wait for responses in ms, must be non-negative. The implementation
     *                is free to use a lower value if appropriate (common reasons for this are a lower request or
     *                metadata update timeout)
     * @param now The current time in ms
     * @throws IllegalStateException If a request is sent to an unready node
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * 断开与特定节点的连接(如果有的话)。
     * 此连接的任何挂起的ClientRequests都将收到断开请求。
     *
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    void disconnect(String nodeId);

    /**
     * 关闭特定节点的连接(如果有的话)。
     * 连接上的所有请求将被清除。对于已清除的请求，不会调用ClientRequest回调，也不会从poll()返回它们。
     *
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    void close(String nodeId);

    /**
     * 选择未完成请求最少的节点。
     * 此方法将首选具有现有连接的节点，但如果所有现有连接都在使用中，则可能会选择尚未拥有连接的节点。
     *
     * Choose the node with the fewest outstanding requests. This method will prefer a node with an existing connection,
     * but will potentially choose a node for which we don't yet have a connection if all existing connections are in
     * use.
     *
     * @param now The current time in ms
     * @return The node with the fewest in-flight requests.
     */
    Node leastLoadedNode(long now);

    /**
     * 尚未返回响应的当前正在处理的请求的数量
     *
     * The number of currently in-flight requests for which we have not yet returned a response
     */
    int inFlightRequestCount();

    /**
     * 如果至少有一个正在处理的请求，则返回true，否则返回false。
     *
     * Return true if there is at least one in-flight request and false otherwise.
     */
    boolean hasInFlightRequests();

    /**
     * 获取特定节点的全部动态请求
     *
     * Get the total in-flight requests for a particular node
     *
     * @param nodeId The id of the node
     */
    int inFlightRequestCount(String nodeId);

    /**
     * 如果某个节点至少有一个正在处理的请求，则返回true，否则返回false。
     *
     * Return true if there is at least one in-flight request for a particular node and false otherwise.
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * 如果至少有一个节点连接处于就绪状态且未被调节，则返回true。否则返回false。
     *
     * Return true if there is at least one node with connection in the READY state and not throttled. Returns false
     * otherwise.
     *
     * @param now the current time
     */
    boolean hasReadyNodes(long now);

    /**
     * 如果客户端当前被阻塞等待I/O，则唤醒它
     *
     * Wake up the client if it is currently blocked waiting for I/O
     */
    void wakeup();

    /**
     * 创建一个新的ClientRequest。
     *
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs, boolean expectResponse);

    /**
     * 创建一个新的ClientRequest。
     *
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     * @param requestTimeoutMs Upper bound time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may get cancelled sooner if the socket disconnects
     *                         for any reason including if another pending request to the same node timed out first.
     * @param callback the callback to invoke when we get a response
     * @param initialPrincipalName the initial client principal name, when building a forward request
     * @param initialClientId the initial client id, when building a forward request
     */
    ClientRequest newClientRequest(String nodeId,
                                   AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs,
                                   boolean expectResponse,
                                   int requestTimeoutMs,
                                   String initialPrincipalName,
                                   String initialClientId,
                                   RequestCompletionHandler callback);

    /**
     * 关闭client。
     *
     * Initiates shutdown of this client. This method may be invoked from another thread while this
     * client is being polled. No further requests may be sent using the client. The current poll()
     * will be terminated using wakeup(). The client should be explicitly shutdown using {@link #close()}
     * after poll returns. Note that {@link #close()} should not be invoked concurrently while polling.
     */
    void initiateClose();

    /**
     * 检测client是否处于未关闭状态
     * 如果客户端仍处于活动状态，则返回true。
     * 如果为该客户端调用了{@link #initiateClose()}或{@link #close()}，则返回false。
     *
     * Returns true if the client is still active. Returns false if {@link #initiateClose()} or {@link #close()}
     * was invoked for this client.
     */
    boolean active();

}
