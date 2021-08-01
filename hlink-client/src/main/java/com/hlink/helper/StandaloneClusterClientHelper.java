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
package com.hlink.helper;


import com.hlink.job.JobDeployer;
import com.hlink.options.Options;
import com.hlink.util.JobGraphUtil;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class StandaloneClusterClientHelper implements ClusterClientHelper {

    private static final Logger LOG = LoggerFactory.getLogger(YarnSessionClusterClientHelper.class);

    @Override
    public ClusterClient submit(JobDeployer jobDeployer) throws Exception {

        Options launcherOptions = jobDeployer.getLauncherOptions();
        List<String> programArgs = jobDeployer.getProgramArgs();
        Configuration flinkConf = launcherOptions.loadFlinkConfiguration();

        try (StandaloneClusterDescriptor standaloneClusterDescriptor = new StandaloneClusterDescriptor(flinkConf)) {
            ClusterClient<StandaloneClusterId> client = standaloneClusterDescriptor.retrieve(StandaloneClusterId.getInstance()).getClusterClient();
            JobGraph jobGraph = JobGraphUtil.buildJobGraph(launcherOptions, programArgs.toArray(new String[0]));
            JobID jobID = (JobID) client.submitJob(jobGraph).get();
            LOG.info("submit job successfully, jobID = {}", jobID);
            return client;
        }
    }
}
