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


import com.hlink.helper.MyLocalStreamEnvironmentHelper;
import com.hlink.conf.CodeConf;
import com.hlink.enums.OperatorType;
import com.hlink.exception.HlinkRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import static com.hlink.constants.ConstantValue.POINT_SYMBOL;


@Slf4j
public class PluginUtil {
    public static final String FORMATS_SUFFIX = "formats";

    public static final String READER_SUFFIX = "reader";
    private static final String JAR_SUFFIX = ".jar";
    public static final String SOURCE_SUFFIX = "source";
    public static final String WRITER_SUFFIX = "writer";
    public static final String SINK_SUFFIX = "sink";
    public static final String GENERIC_SUFFIX = "Factory";
    public static final String METRIC_SUFFIX = "metrics";
    public static final String DEFAULT_METRIC_PLUGIN = "prometheus";
    private static final String SP = File.separator;

    private static final String PACKAGE_PREFIX = "com.dtstack.flinkx.connector.";
    private static final String METRIC_PACKAGE_PREFIX = "com.dtstack.flinkx.metrics.";
    private static final String METRIC_REPORT_PREFIX = "Report";

    private static final String JAR_PREFIX = "flinkx";

    private static final String FILE_PREFIX = "file:";

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    /**
     * ????????????jar???
     *
     * @param pluginDir         ???????????????
     * @param factoryIdentifier SQL??????????????????????????????kafka
     * @return
     * @throws MalformedURLException
     */
    public static URL[] getPluginJarUrls(String pluginDir, String factoryIdentifier) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        File dirFile = new File(pluginDir);

        if (!dirFile.exists() || !dirFile.isDirectory()) {
            throw new RuntimeException("plugin path:" + pluginDir + " is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if (files == null || files.length == 0) {
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for (File file : files) {
            URL pluginJarUrl = file.toURI().toURL();
            urlList.add(pluginJarUrl);
        }

        if (urlList.size() == 0) {
            throw new RuntimeException("no match jar in :" + pluginDir + " directory ???factoryIdentifier is :" + factoryIdentifier);
        }

        return urlList.toArray(new URL[0]);
    }

    /**
     * ????????????????????????????????????
     *
     * @param pluginName       ??????????????????: kafkareader???kafkasource???
     * @param pluginRoot
     * @param remotePluginPath
     * @return
     */
    public static Set<URL> getJarFileDirPath(String pluginName, String pluginRoot, String remotePluginPath) {
        Set<URL> urlList = new HashSet<>();

        String pluginPath = Objects.isNull(remotePluginPath) ? pluginRoot : remotePluginPath;
        String name = pluginName.replace(READER_SUFFIX, "")
                .replace(SOURCE_SUFFIX, "")
                .replace(WRITER_SUFFIX, "")
                .replace(SINK_SUFFIX, "");

        try {
            String pluginJarPath = pluginRoot + SP + name;
            // ??????jar???????????????????????????URL??????
            for (String jarName : getJarNames(new File(pluginJarPath))) {
                urlList.add(new URL(FILE_PREFIX + pluginPath + SP + name + SP + jarName));
            }
            return urlList;
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ?????????????????????????????????jar?????????
     *
     * @param pluginPath
     * @return
     */
    private static List<String> getJarNames(File pluginPath) {
        List<String> jarNames = new ArrayList<>();
        if (pluginPath.exists() && pluginPath.isDirectory()) {
            File[] jarFiles = pluginPath.listFiles((dir, name) ->
                    name.toLowerCase().startsWith(JAR_PREFIX) && name.toLowerCase().endsWith(".jar"));

            if (Objects.nonNull(jarFiles) && jarFiles.length > 0) {
                Arrays.stream(jarFiles).forEach(item -> jarNames.add(item.getName()));
            }
        }
        return jarNames;
    }

    /**
     * ???????????????????????????????????????
     *
     * @param pluginName   ??????kafkareader
     * @param operatorType ????????????
     * @return
     */
    public static String getPluginClassName(String pluginName, OperatorType operatorType) {
        String pluginClassName;
        switch (operatorType) {
            case metric:
                pluginClassName = appendMetricClass(pluginName);
                break;
            case source:
                String sourceName = pluginName.replace(READER_SUFFIX, SOURCE_SUFFIX);
                pluginClassName = camelize(sourceName, SOURCE_SUFFIX);
                break;
            case sink:
                String sinkName = pluginName.replace(WRITER_SUFFIX, SINK_SUFFIX);
                pluginClassName = camelize(sinkName, SINK_SUFFIX);
                break;
            default:
                throw new HlinkRuntimeException("unknown operatorType: " + operatorType);
        }

        return pluginClassName;
    }

    /**
     * ??????????????????????????????
     *
     * @param pluginName ????????????????????????binlogsource
     * @param suffix     ???????????????????????????source???sink
     * @return ?????????????????????????????????com.dtstack.flinkx.connector.binlog.source.BinlogSourceFactory
     */
    private static String camelize(String pluginName, String suffix) {
        int pos = pluginName.indexOf(suffix);
        String left = pluginName.substring(0, pos);
        left = left.toLowerCase();
        suffix = suffix.toLowerCase();
        StringBuilder sb = new StringBuilder(32);
        sb.append(PACKAGE_PREFIX);
        sb.append(left).append(POINT_SYMBOL).append(suffix).append(POINT_SYMBOL);
        sb.append(left.substring(0, 1).toUpperCase()).append(left.substring(1));
        sb.append(suffix.substring(0, 1).toUpperCase()).append(suffix.substring(1));
        sb.append(GENERIC_SUFFIX);
        return sb.toString();
    }

    private static String appendMetricClass(String pluginName) {
        StringBuilder sb = new StringBuilder(32);
        sb.append(METRIC_PACKAGE_PREFIX).append(pluginName.toLowerCase(Locale.ENGLISH)).append(POINT_SYMBOL);
        sb.append(pluginName.substring(0, 1).toUpperCase()).append(pluginName.substring(1).toLowerCase());
        sb.append(METRIC_REPORT_PREFIX);
        return sb.toString();
    }

    /**
     * ???????????????????????????????????????env???
     *
     * @param config
     * @param env
     */
    public static void registerPluginUrlToCachedFile(CodeConf config, StreamExecutionEnvironment env) {
        Set<URL> urlSet = new HashSet<>();
        Set<URL> coreUrlList = getJarFileDirPath("", config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> formatsUrlList = getJarFileDirPath(FORMATS_SUFFIX, config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> sourceUrlList = getJarFileDirPath(config.getReader().getName(), config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> sinkUrlList = getJarFileDirPath(config.getWriter().getName(), config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> metricUrlList = getJarFileDirPath(
                config.getMetricPluginConf().getPluginName(),
                config.getPluginRoot() + SP + METRIC_SUFFIX,
                config.getRemotePluginPath());
        urlSet.addAll(coreUrlList);
        urlSet.addAll(formatsUrlList);
        urlSet.addAll(sourceUrlList);
        urlSet.addAll(sinkUrlList);
        urlSet.addAll(metricUrlList);

        int i = 0;
        for (URL url : urlSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }

        if (env instanceof MyLocalStreamEnvironmentHelper) {
            ((MyLocalStreamEnvironmentHelper) env).setClasspaths(new ArrayList<>(urlSet));
            if (CollectionUtils.isNotEmpty(coreUrlList)) {
                try {
                    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                    add.setAccessible(true);
                    add.invoke(contextClassLoader, new ArrayList<>(coreUrlList).get(0));
                } catch (Exception e) {
                    log.warn("cannot add core jar into contextClassLoader, coreUrlList = {}", GsonUtil.GSON.toJson(coreUrlList), e);
                }
            }
        }
    }
}
