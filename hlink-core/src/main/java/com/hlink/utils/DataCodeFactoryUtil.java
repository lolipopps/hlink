/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hlink.utils;

import com.hlink.classload.ClassLoaderManager;
import com.hlink.conf.CodeConf;
import com.hlink.conf.FlinkCommonConf;
import com.hlink.conf.MetricConf;
import com.hlink.enums.OperatorType;
import com.hlink.exception.HlinkRuntimeException;
import com.hlink.metrics.CustomReporter;
import com.hlink.sink.SinkFactory;
import com.hlink.source.SourceFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Set;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/27
 */
public class DataCodeFactoryUtil {

    public static SourceFactory discoverSource(CodeConf config, StreamExecutionEnvironment env) {
        try {
            // 哪类数据源
            String pluginName = config.getJob().getReader().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.source);
            Set<URL> urlList = PluginUtil.getJarFileDirPath(pluginName, config.getPluginRoot(), null);
            urlList.addAll(PluginUtil.getJarFileDirPath(PluginUtil.FORMATS_SUFFIX, config.getPluginRoot(), null));

            return ClassLoaderManager.newInstance(
                    urlList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(CodeConf.class, StreamExecutionEnvironment.class);
                        return (SourceFactory) constructor.newInstance(config, env);
                    });
        } catch (Exception e) {
            throw new HlinkRuntimeException(e);
        }
    }

    public static CustomReporter discoverMetric(FlinkCommonConf flinkxCommonConf, RuntimeContext context, boolean makeTaskFailedWhenReportFailed) {
        try {
            String pluginName = flinkxCommonConf.getMetricPluginName() ;
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.metric);
            Set<URL> urlList = PluginUtil.getJarFileDirPath(pluginName, flinkxCommonConf.getMetricPluginRoot(), null);
            MetricConf metricParam = new MetricConf(context,makeTaskFailedWhenReportFailed,flinkxCommonConf.getMetricProps());
            return ClassLoaderManager.newInstance(
                    urlList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor constructor =
                                clazz.getConstructor(
                                        MetricConf.class);
                        return (CustomReporter) constructor.newInstance(metricParam);
                    });
        } catch (Exception e) {
            throw new HlinkRuntimeException(e);
        }
    }

    public static SinkFactory discoverSink(CodeConf config) {
        try {
            String pluginName = config.getJob().getContent().get(0).getWriter().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.sink);
            Set<URL> urlList = PluginUtil.getJarFileDirPath(pluginName, config.getPluginRoot(), null);
            urlList.addAll(PluginUtil.getJarFileDirPath(PluginUtil.FORMATS_SUFFIX, config.getPluginRoot(), null));

            return ClassLoaderManager.newInstance(
                    urlList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(CodeConf.class);
                        return (SinkFactory) constructor.newInstance(config);
                    });
        } catch (Exception e) {
            throw new HlinkRuntimeException(e);
        }
    }
}
