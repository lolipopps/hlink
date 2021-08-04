/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hlink;

import com.hlink.enums.ClusterMode;
import com.hlink.helper.*;
import com.hlink.job.JobDeployer;
import com.hlink.options.OptionParser;
import com.hlink.options.Options;
import com.hlink.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.configuration.ConfigConstants;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class Launcher {

    public static final String KEY_FLINKX_HOME = "FLINKX_HOME";
    public static final String KEY_FLINK_HOME = "FLINK_HOME";
    public static final String KEY_HADOOP_HOME = "HADOOP_HOME";

    public static final String PLUGINS_DIR_NAME = "flinkxplugins";

    public static void main(String[] args) throws Exception {
// 	-mode local \
//	-jobType sync \
//	-job flinkx-local-test/src/main/demo/json/stream/stream.json
// E:\code\flinkx\flinkx-examples\sql\kafka\kafka_kafka.sql
//	-pluginRoot flinkxplugins

        ArrayList<String> arg = new ArrayList<>();
        arg.add("-mode");
        arg.add("local");
        arg.add("-jobType");
        arg.add("sql");
//        arg.add("-job");arg.add("hlink-examples/json/kafka/kafka_stream.json");
        arg.add("-job");
        arg.add("hlink-examples/sql/kafka/kafka_upsert-kafka.sql");
        arg.add("-pluginRoot");
        arg.add("flinkxplugins");
        String[] ar = arg.toArray(new String[arg.size()]);
        OptionParser optionParser = new OptionParser(ar);
        Options launcherOptions = optionParser.getOptions();

        findDefaultConfigDir(launcherOptions);
        log.info("-------------begin--------------------");
        List<String> argList = optionParser.getProgramExeArgList();

        // 将argList转化为HashMap，方便通过参数名称来获取参数值
        HashMap<String, String> temp = new HashMap<>(16);
        for (int i = 0; i < argList.size(); i += 2) {
            temp.put(argList.get(i), argList.get(i + 1));
        }
        // 对json中的值进行修改
        String s = temp.get("-p");
        if (StringUtils.isNotBlank(s)) {
            HashMap<String, String> parameter = JsonUtil.CommandTransform(s);
            temp.put("-job", JsonUtil.JsonValueReplace(temp.get("-job"), parameter));
        }

        // 清空list，填充修改后的参数值
        argList.clear();
        for (int i = 0; i < temp.size(); i++) {
            argList.add(temp.keySet().toArray()[i].toString());
            argList.add(temp.values().toArray()[i].toString());
        }

        JobDeployer jobDeployer = new JobDeployer(launcherOptions, argList);

        ClusterClientHelper clusterClientHelper = null;
        switch (ClusterMode.getByName(launcherOptions.getMode())) {
            case local:
                clusterClientHelper = new LocalClusterClientHelper();
                break;
            case standalone:
                clusterClientHelper = new StandaloneClusterClientHelper();
                break;
            case yarnSession:
                clusterClientHelper = new YarnSessionClusterClientHelper();
                break;
            case yarnPerJob:
                clusterClientHelper = new YarnPerJobClusterClientHelper();
                break;
            case yarnApplication:
                throw new ClusterDeploymentException(
                        "Application Mode not supported by Yarn deployments.");
//            case kubernetesSession:
//                clusterClientHelper = new KubernetesSessionClusterClientHelper();
//                break;
//            case kubernetesPerJob:
//                throw new ClusterDeploymentException(
//                        "Per-Job Mode not supported by Kubernetes deployments.");
//            case kubernetesApplication:
//                clusterClientHelper = new KubernetesApplicationClusterClientHelper();
//                break;
            default:
                throw new ClusterDeploymentException(launcherOptions.getMode() + " Mode not supported.");
        }
        clusterClientHelper.submit(jobDeployer);
    }

    private static void findDefaultConfigDir(Options launcherOptions) {
        findDefaultPluginRoot(launcherOptions);

        if (ClusterMode.local.name().equalsIgnoreCase(launcherOptions.getMode())) {
            return;
        }

        findDefaultFlinkConf(launcherOptions);
        findDefaultHadoopConf(launcherOptions);
    }

    private static void findDefaultHadoopConf(Options launcherOptions) {
        if (StringUtils.isNotEmpty(launcherOptions.getYarnconf())) {
            return;
        }

        String hadoopHome = getSystemProperty(KEY_HADOOP_HOME);
        if (StringUtils.isNotEmpty(hadoopHome)) {
            hadoopHome = hadoopHome.trim();
            if (hadoopHome.endsWith(File.separator)) {
                hadoopHome = hadoopHome.substring(0, hadoopHome.lastIndexOf(File.separator));
            }

            launcherOptions.setYarnconf(hadoopHome + "/etc/hadoop");
        }
    }

    private static void findDefaultFlinkConf(Options launcherOptions) {

        String flinkHome = getSystemProperty(KEY_FLINK_HOME);
        if (StringUtils.isNotEmpty(flinkHome)) {
            flinkHome = flinkHome.trim();
            if (flinkHome.endsWith(File.separator)) {
                flinkHome = flinkHome.substring(0, flinkHome.lastIndexOf(File.separator));
            }

            if (StringUtils.isEmpty(launcherOptions.getFlinkconf())) {
                launcherOptions.setFlinkconf(flinkHome + "/conf");
            }

            if (StringUtils.isEmpty(launcherOptions.getFlinkLibJar())) {
                launcherOptions.setFlinkLibJar(flinkHome + "/lib");
            }
        }
    }

    private static void findDefaultPluginRoot(Options launcherOptions) {
        String pluginRoot = launcherOptions.getPluginRoot();
        if (StringUtils.isEmpty(pluginRoot)) {
            String flinkxHome = getSystemProperty(KEY_FLINKX_HOME);
            if (StringUtils.isNotEmpty(flinkxHome)) {
                flinkxHome = flinkxHome.trim();
                if (flinkxHome.endsWith(File.separator)) {
                    pluginRoot = flinkxHome + PLUGINS_DIR_NAME;
                } else {
                    pluginRoot = flinkxHome + File.separator + PLUGINS_DIR_NAME;
                }

                launcherOptions.setPluginRoot(pluginRoot);
            }
        }
        System.setProperty(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginRoot);
    }

    private static String getSystemProperty(String name) {
        String property = System.getenv(name);
        if (StringUtils.isEmpty(property)) {
            property = System.getProperty(name);
        }

        return property;
    }
}
