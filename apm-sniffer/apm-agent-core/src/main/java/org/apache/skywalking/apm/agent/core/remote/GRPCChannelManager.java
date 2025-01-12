/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.remote;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.IS_RESOLVE_DNS_PERIODICALLY;

/**
 * 这个服务是agent 到 oap的大动脉，也就是网络链接
 */
@DefaultImplementor
public class GRPCChannelManager implements BootService, Runnable {
    private static final ILog LOGGER = LogManager.getLogger(GRPCChannelManager.class);

    private volatile GRPCChannel managedChannel = null;//Agent与OAP之间的网络链接
    private volatile ScheduledFuture<?> connectCheckFuture;//网络链接状态定时检查任务调度器
    private volatile boolean reconnect = true; // 当前网络状态是否需要重链
    private final Random random = new Random();
    private final List<GRPCChannelListener> listeners = Collections.synchronizedList(new LinkedList<>());
    private volatile List<String> grpcServers;//OAP地址列表
    private volatile int selectedIdx = -1; //上一次选择的OAP地址的下标
    private volatile int reconnectCount = 0;

    @Override
    public void prepare() {

    }

    /**
     * agent 收集到的数据 上报OAP
     */
    @Override
    public void boot() {
        // 是否有配置OAP地址
        if (Config.Collector.BACKEND_SERVICE.trim().length() == 0) {
            LOGGER.error("Collector server addresses are not set.");
            LOGGER.error("Agent will not uplink any data.");
            return;
        }
        // 解析 OAP 实例地址
        grpcServers = Arrays.asList(Config.Collector.BACKEND_SERVICE.split(","));
        // 开启一个检测 Channel 连接状态并适时重启的定时任务, 默认30s
        connectCheckFuture = Executors.newSingleThreadScheduledExecutor(
             // 自定义线程工厂，用于为线程命名并开启守护线程
             new DefaultNamedThreadFactory("GRPCChannelManager")
        ).scheduleAtFixedRate(
            new RunnableWithExceptionProtection(
                this,//传的this，还是执行执行的run方法
                // 线程异常时的回调
                t -> LOGGER.error("unexpected exception.", t)
            ), 0, Config.Collector.GRPC_CHANNEL_CHECK_INTERVAL, TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {
        if (connectCheckFuture != null) {
            connectCheckFuture.cancel(true);
        }
        if (managedChannel != null) {
            managedChannel.shutdownNow();
        }
        LOGGER.debug("Selected collector grpc service shutdown.");
    }

    @Override
    public void run() {
        LOGGER.debug("Selected collector grpc service running, reconnect:{}.", reconnect);
        //是否周期性的解析dns && 当前网络链接是否需要重新链接
        if (IS_RESOLVE_DNS_PERIODICALLY && reconnect) {
            grpcServers = Arrays.stream(Config.Collector.BACKEND_SERVICE.split(","))
                    .filter(StringUtil::isNotBlank)
                    .map(eachBackendService -> eachBackendService.split(":"))
                    .filter(domainPortPairs -> {
                        if (domainPortPairs.length < 2) {
                            LOGGER.debug("Service address [{}] format error. The expected format is IP:port", domainPortPairs[0]);
                            return false;
                        }
                        return true;
                    })
                    .flatMap(domainPortPairs -> {
                        try {
                            return Arrays
                                    // 找到domain(域名) 对应的所有ip：端口
                                    .stream(InetAddress.getAllByName(domainPortPairs[0]))//通过域名找ip地址
                                    .map(InetAddress::getHostAddress)
                                    .map(ip -> String.format("%s:%s", ip, domainPortPairs[1]));
                        } catch (Throwable t) {
                            LOGGER.error(t, "Failed to resolve {} of backend service.", domainPortPairs[0]);
                        }
                        return Stream.empty();
                    })
                    .distinct()
                    .collect(Collectors.toList());
        }

        if (reconnect) {//需要重新链接
            if (grpcServers.size() > 0) {//且后端地址有值
                String server = "";
                try {
                    int index = Math.abs(random.nextInt()) % grpcServers.size();// 随机选择一个地址
                    if (index != selectedIdx) {// 这次选择的地址不等于上一次选择的地址
                        selectedIdx = index;

                        server = grpcServers.get(index);
                        String[] ipAndPort = server.split(":");

                        if (managedChannel != null) {
                            managedChannel.shutdownNow();//出问题的链接shutDown
                        }

                        // 新建链接
                        managedChannel = GRPCChannel.newBuilder(ipAndPort[0], Integer.parseInt(ipAndPort[1]))
                                                    .addManagedChannelBuilder(new StandardChannelBuilder())
                                                    .addManagedChannelBuilder(new TLSChannelBuilder())
                                                    .addChannelDecorator(new AgentIDDecorator())
                                                    .addChannelDecorator(new AuthenticationDecorator())
                                                    .build();
                        reconnectCount = 0;
                        reconnect = false;
                        notify(GRPCChannelStatus.CONNECTED);//通知所有使用这个网络链接的BootService
                    } else if (managedChannel.isConnected(++reconnectCount > Config.Agent.FORCE_RECONNECTION_PERIOD)) {
                        // Reconnect to the same server is automatically done by GRPC,
                        // therefore we are responsible to check the connectivity and
                        // set the state and notify listeners
                        reconnectCount = 0;
                        reconnect = false;
                        notify(GRPCChannelStatus.CONNECTED);
                    }

                    return;
                } catch (Throwable t) {
                    LOGGER.error(t, "Create channel to {} fail.", server);
                }
            }

            LOGGER.debug(
                "Selected collector grpc service is not available. Wait {} seconds to retry",
                Config.Collector.GRPC_CHANNEL_CHECK_INTERVAL
            );
        }
    }

    public void addChannelListener(GRPCChannelListener listener) {
        listeners.add(listener);
    }

    public Channel getChannel() {
        return managedChannel.getChannel();
    }

    /**
     * If the given exception is triggered by network problem, connect in background.
     */
    public void reportError(Throwable throwable) {
        if (isNetworkError(throwable)) {
            reconnect = true;
            notify(GRPCChannelStatus.DISCONNECT);
        }
    }

    private void notify(GRPCChannelStatus status) {
        for (GRPCChannelListener listener : listeners) {
            try {
                listener.statusChanged(status);
            } catch (Throwable t) {
                LOGGER.error(t, "Fail to notify {} about channel connected.", listener.getClass().getName());
            }
        }
    }

    private boolean isNetworkError(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) throwable;
            return statusEquals(
                statusRuntimeException.getStatus(), Status.UNAVAILABLE, Status.PERMISSION_DENIED,
                Status.UNAUTHENTICATED, Status.RESOURCE_EXHAUSTED, Status.UNKNOWN
            );
        }
        return false;
    }

    private boolean statusEquals(Status sourceStatus, Status... potentialStatus) {
        for (Status status : potentialStatus) {
            if (sourceStatus.getCode() == status.getCode()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int priority() {
        return Integer.MAX_VALUE;
    }
}
