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

package org.apache.skywalking.apm.agent.core.sampling;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.conf.dynamic.ConfigurationDiscoveryService;
import org.apache.skywalking.apm.agent.core.conf.dynamic.watcher.SamplingRateWatcher;
import org.apache.skywalking.apm.agent.core.context.trace.TraceSegment;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

/**
 * 这些BootService 应该都是单例,需要看一下怎么保证单例的
 *
 * 采样服务
 * <p>控制如何去采样 TraceSegment
 * The <code>SamplingService</code> take charge of how to sample the {@link TraceSegment}. Every {@link TraceSegment}s
 * have been traced, but, considering CPU cost of serialization/deserialization, and network bandwidth, the agent do NOT
 * send all of them to collector, if SAMPLING is on.
 *
 * 每个TraceSegment 都会被采集(追踪到的),但是考虑到序列化与反序列化时的服务器损耗,还有网络带宽的损耗, agent不会将所有的TraceSegment都发送给OPA服务
 *
 * <p>
 * By default, SAMPLING is on, and  {@link Config.Agent#SAMPLE_N_PER_3_SECS }
 */
@DefaultImplementor // 表示BootService的默认实现
public class SamplingService implements BootService {
    private static final ILog LOGGER = LogManager.getLogger(SamplingService.class);

    private volatile boolean on = false;
    private volatile AtomicInteger samplingFactorHolder;//累加三秒类已经采样的次数
    private volatile ScheduledFuture<?> scheduledFuture; // 每3秒重置一次  samplingFactorHolder

    private SamplingRateWatcher samplingRateWatcher;
    private ScheduledExecutorService service;

    @Override
    public void prepare() {
    }

    @Override
    public void boot() {
        service = Executors.newSingleThreadScheduledExecutor(
                new DefaultNamedThreadFactory("SamplingService"));
        samplingRateWatcher = new SamplingRateWatcher("agent.sample_n_per_3_secs", this);
        /**
         * 注册采样率配置监听器, 具体配置监听的实现在{@link ConfigurationDiscoveryService}
         */
        ServiceManager.INSTANCE.findService(ConfigurationDiscoveryService.class)
                               .registerAgentConfigChangeWatcher(samplingRateWatcher);

        handleSamplingRateChanged();
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    /**    如果采样机制没开启,则每个采样都会发送给后端OAP服务,如果采样机制开启了,则会触发3秒内最大上报采样量的限制, 这也是奇葩....
     * When the sampling mechanism is on and the sample limited is not reached, the trace segment
     * should be traced. If the sampling mechanism is off, it means that all trace segments should
     * be traced.
     * @param operationName The first operation name of the new tracing context.
     * @return true if should sample this trace segment. When sampling mechanism is on, return true if sample limited is not reached.
     */
    public boolean trySampling(String operationName) {
        if (on) {
            int factor = samplingFactorHolder.get();
            //当前采样数 小于 配置的 3秒内最大上报采样数
            if (factor < samplingRateWatcher.getSamplingRate()) {
                return samplingFactorHolder.compareAndSet(factor, factor + 1);
            } else {
                return false;
            }
        }
        // 如果采样机制没开启,则每个采样都会发送给后端OAP服务
        return true;
    }

    /**
     * Increase the sampling factor by force, to avoid sampling too many traces. If many distributed traces require
     * sampled, the trace beginning at local, has less chance to be sampled.
     */
    public void forceSampled() {
        if (on) {
            samplingFactorHolder.incrementAndGet();
        }
    }

    private void resetSamplingFactor() {
        samplingFactorHolder = new AtomicInteger(0);
    }

    /**
     * Handle the samplingRate changed. 采样率
     */
    public void handleSamplingRateChanged() {
        if (samplingRateWatcher.getSamplingRate() > 0) {
            if (!on) {
                on = true;
                this.resetSamplingFactor();
                scheduledFuture = service.scheduleAtFixedRate(new RunnableWithExceptionProtection(
                    this::resetSamplingFactor, t -> LOGGER.error("unexpected exception.", t)), 0, 3, TimeUnit.SECONDS);
                LOGGER.debug(
                    "Agent sampling mechanism started. Sample {} traces in 3 seconds.",
                    samplingRateWatcher.getSamplingRate()
                );
            }
        } else {
            if (on) {
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(true);
                }
                on = false;
            }
        }
    }
}
