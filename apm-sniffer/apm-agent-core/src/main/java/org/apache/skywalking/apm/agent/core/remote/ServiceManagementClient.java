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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.commands.CommandService;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.jvm.LoadedLibraryCollector;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.os.OSUtil;
import org.apache.skywalking.apm.agent.core.util.InstanceJsonPropertiesUtil;
import org.apache.skywalking.apm.network.common.v3.Commands;
import org.apache.skywalking.apm.network.common.v3.KeyStringValuePair;
import org.apache.skywalking.apm.network.management.v3.InstancePingPkg;
import org.apache.skywalking.apm.network.management.v3.InstanceProperties;
import org.apache.skywalking.apm.network.management.v3.ManagementServiceGrpc;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.GRPC_UPSTREAM_TIMEOUT;


/**
 *  自报家门
 *
 *  1:将当前 Agent client 的基本信息os，jvm 信息 汇报给OAP
 *      agent挂载到一个项目（服务）上，那这个项目（服务）就是agent client
 *  2: 和OAP 保持心跳
 *
 */

@DefaultImplementor
public class ServiceManagementClient implements BootService, Runnable, GRPCChannelListener {
    private static final ILog LOGGER = LogManager.getLogger(ServiceManagementClient.class);
    private static List<KeyStringValuePair> SERVICE_INSTANCE_PROPERTIES;// 保存着agent client的信息

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;// 当前网络状态
    private volatile ManagementServiceGrpc.ManagementServiceBlockingStub managementServiceBlockingStub; //网络服务，远程调用
    private volatile ScheduledFuture<?> heartbeatFuture;
    private volatile AtomicInteger sendPropertiesCounter = new AtomicInteger(0); //Agent Client 信息次数发送计数器

    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            // grpc 中的stub 就等价于 dubbo中的interface定义， 这里指的是在protobuf中定义的XxxService（apm-protocol/apm-network/src/main/proto/management/Management.proto:32）
            managementServiceBlockingStub = ManagementServiceGrpc.newBlockingStub(channel);
        } else {
            managementServiceBlockingStub = null;
        }
        this.status = status;
    }

    @Override
    public void prepare() {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);

        SERVICE_INSTANCE_PROPERTIES = InstanceJsonPropertiesUtil.parseProperties();
    }

    @Override
    public void boot() {
        //心跳定时任务
        heartbeatFuture = Executors.newSingleThreadScheduledExecutor(
            new DefaultNamedThreadFactory("ServiceManagementClient")
        ).scheduleAtFixedRate(
            new RunnableWithExceptionProtection(
                this,
                t -> LOGGER.error("unexpected exception.", t)
            ), 0, Config.Collector.HEARTBEAT_PERIOD,
            TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void shutdown() {
        heartbeatFuture.cancel(true);
    }

    @Override
    public void run() {
        LOGGER.debug("ServiceManagementClient running, status:{}.", status);

        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            try {
                if (managementServiceBlockingStub != null) {
                    // Math.abs(sendPropertiesCounter.getAndAdd(1)) 这个取绝对值的原因：
                    // 一个agent client 发送消息的次数可能会非常多，当超出integer的最大值的时候，会变成负数， 所有取绝对值
                    //% 取余的原因：
                    // 定时任务30秒调度一次，调度一次，sendPropertiesCounter.getAndAdd(1))的值 加1
                    // 只有第0次调度，第10次调度，第20次调度。。。。 该表达式才等于true
                    // 也就是0*30秒 ， 10*30秒， 第20*30秒 会发送一次数据到OAP, 也就是5分钟发送一次数据
                    // todo 不过为啥不干脆5分钟调度一次呢？
                    if (Math.abs(
                        sendPropertiesCounter.getAndAdd(1)) % Config.Collector.PROPERTIES_REPORT_PERIOD_FACTOR == 0) {

                        managementServiceBlockingStub
                            .withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS)
                            .reportInstanceProperties(InstanceProperties.newBuilder()
                                                                        .setService(Config.Agent.SERVICE_NAME)
                                                                        .setServiceInstance(Config.Agent.INSTANCE_NAME)
                                                                        .addAllProperties(OSUtil.buildOSInfo(
                                                                            Config.OsInfo.IPV4_LIST_SIZE))
                                                                        .addAllProperties(SERVICE_INSTANCE_PROPERTIES)
                                                                        .addAllProperties(
                                                                            LoadedLibraryCollector.buildJVMInfo())
                                                                        .build());
                    } else {
                        // 不在上报周期内，则发送心跳包
                        final Commands commands = managementServiceBlockingStub.withDeadlineAfter(
                            GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                        ).keepAlive(InstancePingPkg.newBuilder()
                                                   .setService(Config.Agent.SERVICE_NAME)
                                                   .setServiceInstance(Config.Agent.INSTANCE_NAME)
                                                   .build());

                        ServiceManager.INSTANCE.findService(CommandService.class).receiveCommand(commands);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error(t, "ServiceManagementClient execute fail.");
                ServiceManager.INSTANCE.findService(GRPCChannelManager.class).reportError(t);
            }
        }
    }
}
