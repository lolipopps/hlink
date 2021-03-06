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
package com.hlink.util;


import com.hlink.options.Options;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class JobGraphUtil {



    public static JobGraph buildJobGraph(Options launcherOptions, String[] programArgs) throws Exception {
        String pluginRoot = launcherOptions.getPluginRoot();
        String coreJarPath = PluginInfoUtil.getCoreJarPath(pluginRoot);
        File jarFile = new File(coreJarPath);
        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
        if (StringUtils.isNotEmpty(launcherOptions.getS())) {
            savepointRestoreSettings = SavepointRestoreSettings.forPath(launcherOptions.getS());
        }
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(jarFile)
                .setEntryPointClassName(PluginInfoUtil.getMainClass())
                .setConfiguration(launcherOptions.loadFlinkConfiguration())
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .setArguments(programArgs)
                .build();
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, launcherOptions.loadFlinkConfiguration(), Integer.parseInt(launcherOptions.getParallelism()), false);
        List<URL> pluginClassPath = jobGraph.getUserArtifacts()
                .entrySet()
                .stream()
                .filter(tmp -> tmp.getKey().startsWith("class_path"))
                .map(tmp -> new File(tmp.getValue().filePath))
                .map(file -> {
                    try {
                        return file.toURI().toURL();
                    } catch (MalformedURLException e) {
                        log.error(e.getMessage());
                    }
                    return null;
                })
                .collect(Collectors.toList());
        jobGraph.setClasspaths(pluginClassPath);
        return jobGraph;
    }

    public static PackagedProgram buildProgram(ClusterSpecification clusterSpecification) throws Exception {
        return PackagedProgram.newBuilder()
                .build();
    }
}
