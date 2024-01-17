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

package org.apache.skywalking.apm.agent.core.boot;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.loader.AgentClassLoader;

/**
 * The <code>ServiceManager</code> bases on {@link ServiceLoader}, load all {@link BootService} implementations.
 */
public enum ServiceManager {
    INSTANCE;

    private static final ILog LOGGER = LogManager.getLogger(ServiceManager.class);
    private Map<Class, BootService> bootedServices = Collections.emptyMap();

    public void boot() {
        bootedServices = loadAllServices();

        prepare();
        startup();
        onComplete();
    }

    public void shutdown() {
        bootedServices.values().stream().sorted(Comparator.comparingInt(BootService::priority).reversed()).forEach(service -> {//按优先级倒叙排
            try {
                service.shutdown();
            } catch (Throwable e) {
                LOGGER.error(e, "ServiceManager try to shutdown [{}] fail.", service.getClass().getName());
            }
        });
    }

    /**
     * 非常复杂的逻辑
     * @return
     */
    private Map<Class, BootService> loadAllServices() {
        Map<Class, BootService> bootedServices = new LinkedHashMap<>();
        List<BootService> allServices = new LinkedList<>();
        load(allServices);//加载所有BootService的子类
        for (final BootService bootService : allServices) {
            Class<? extends BootService> bootServiceClass = bootService.getClass();
            boolean isDefaultImplementor = bootServiceClass.isAnnotationPresent(DefaultImplementor.class);
            if (isDefaultImplementor) { // 有 @DefaultImplementor
                if (!bootedServices.containsKey(bootServiceClass)) {
                    bootedServices.put(bootServiceClass, bootService);//将默认实现放到map里面，key： class，value：实现类
                } else {
                    //ignore the default service
                }
            } else {
                OverrideImplementor overrideImplementor = bootServiceClass.getAnnotation(OverrideImplementor.class);
                if (overrideImplementor == null) { // 既没有 @DefaultImplementor 也没有 @OverrideImplementor
                    if (!bootedServices.containsKey(bootServiceClass)) {
                        bootedServices.put(bootServiceClass, bootService);//非覆盖实现也放到map里面，key： class，value：实现类
                    } else {
                        throw new ServiceConflictException("Duplicate service define for :" + bootServiceClass);
                    }
                } else {// 没有 @DefaultImplementor 但是有 @OverrideImplementor
                    Class<? extends BootService> targetService = overrideImplementor.value();
                    if (bootedServices.containsKey(targetService)) {
                        boolean presentDefault = bootedServices.get(targetService)
                                                               .getClass()
                                                               .isAnnotationPresent(DefaultImplementor.class);
                        if (presentDefault) {
                            bootedServices.put(targetService, bootService);
                        } else {
                            throw new ServiceConflictException(
                                "Service " + bootServiceClass + " overrides conflict, " + "exist more than one service want to override :" + targetService);
                        }
                    } else {
                        bootedServices.put(targetService, bootService);
                    }
                }
            }

        }
        return bootedServices;
    }

    private void prepare() {
        bootedServices.values().stream().sorted(Comparator.comparingInt(BootService::priority)).forEach(service -> {
            try {
                service.prepare();
            } catch (Throwable e) {
                LOGGER.error(e, "ServiceManager try to pre-start [{}] fail.", service.getClass().getName());
            }
        });
    }

    private void startup() {
        bootedServices.values().stream().sorted(Comparator.comparingInt(BootService::priority)).forEach(service -> {
            try {
                service.boot();
            } catch (Throwable e) {
                LOGGER.error(e, "ServiceManager try to start [{}] fail.", service.getClass().getName());
            }
        });
    }

    private void onComplete() {
        for (BootService service : bootedServices.values()) {
            try {
                service.onComplete();
            } catch (Throwable e) {
                LOGGER.error(e, "Service [{}] AfterBoot process fails.", service.getClass().getName());
            }
        }
    }

    /**
     * Find a {@link BootService} implementation, which is already started.
     *
     * @param serviceClass class name.
     * @param <T>          {@link BootService} implementation class.
     * @return {@link BootService} instance
     */
    public <T extends BootService> T findService(Class<T> serviceClass) {
        return (T) bootedServices.get(serviceClass);
    }

    /**
     * 到 META-INF/services/org.apache.skywalking.apm.agent.core.boot.BootService 目录下加载服务，也就是BootService的子类
     * @param allServices
     */
    void load(List<BootService> allServices) {
        for (final BootService bootService : ServiceLoader.load(BootService.class, AgentClassLoader.getDefault())) {
            allServices.add(bootService);
        }
    }
}
