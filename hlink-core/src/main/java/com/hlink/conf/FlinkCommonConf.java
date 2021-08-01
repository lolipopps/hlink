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
package com.hlink.conf;

import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Data
public class FlinkCommonConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 速率上限，0代表不限速 */
    private long speedBytes = 0;
    /** 容忍的最大脏数据条数 */
    private int errorRecord = 0;
    /** 容忍的最大脏数据比例，-1代表不校验比例 */
    private int errorPercentage = -1;
    /** 脏数据文件的绝对路径，支持本地和hdfs */
    private String dirtyDataPath;
    /** hdfs时，Hadoop的配置 */
    private Map<String, Object> dirtyDataHadoopConf;
    /** 脏数据对应的字段名称列表 */
    private List<String> fieldNameList = Collections.emptyList();
    /** 是否校验format */
    private boolean checkFormat = true;
    /** 并行度 */
    private Integer parallelism = 1;
    /** table field column conf */
    private List<FieldConf> column;
    /** Number of batches written */
    private int batchSize = 1;
    /** Time when the timer is regularly written to the database */
    private long flushIntervalMills = 10000L;
    /** cp path */
    private String restorePath;

    /** metrics plugin root */
    private String metricPluginRoot;

    /** metrics plugin name */
    private String metricPluginName;

    /** metrics plugin properties */
    private Map<String,Object> metricProps;

    @Override
    public String toString() {
        return "FlinkxCommonConf{" +
                "speedBytes=" + speedBytes +
                ", errorRecord=" + errorRecord +
                ", errorPercentage=" + errorPercentage +
                ", dirtyDataPath='" + dirtyDataPath + '\'' +
                ", dirtyDataHadoopConf=" + dirtyDataHadoopConf +
                ", fieldNameList=" + fieldNameList +
                ", checkFormat=" + checkFormat +
                ", parallelism=" + parallelism +
                ", column=" + column +
                ", batchSize=" + batchSize +
                ", flushIntervalMills=" + flushIntervalMills +
                ", metricPluginRoot='" + metricPluginRoot + '\'' +
                ", metricPluginName='" + metricPluginName + '\'' +
                ", metricProps=" + metricProps +
                ", restorePath=" + restorePath +
                '}';
    }
}
