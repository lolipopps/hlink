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

package com.hlink.source;


import com.hlink.conf.CodeConf;
import com.hlink.conf.FlinkCommonConf;
import com.hlink.conf.SpeedConf;
import com.hlink.converter.RawTypeConvertible;
import com.hlink.utils.PropertiesUtil;
import com.hlink.utils.TableUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;

/**
 * 输入端定义
 */
public abstract class SourceFactory implements RawTypeConvertible {

    protected StreamExecutionEnvironment env;
    protected CodeConf codeConf;
    protected TypeInformation<RowData> typeInformation;
    protected boolean useAbstractBaseColumn = true;

    protected SourceFactory(CodeConf codeConf, StreamExecutionEnvironment env) {
        this.env = env;
        this.codeConf = codeConf;

        if (codeConf.getTransformer() == null || StringUtils.isBlank(codeConf.getTransformer().getTransformSql())) {
            typeInformation = TableUtil.getTypeInformation(Collections.emptyList(), getRawTypeConverter());
        } else {
            typeInformation = TableUtil.getTypeInformation(codeConf.getReader().getFieldList(), getRawTypeConverter());
            useAbstractBaseColumn = false;
        }
    }

    /**
     * Build the read data flow object
     *
     * @return DataStream
     */
    public abstract DataStream<RowData> createSource();

    protected DataStream<RowData> createInput(InputFormat<RowData, InputSplit> inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
        DtInputFormatSourceFunction<RowData> function = new DtInputFormatSourceFunction<>(inputFormat, typeInformation);
        return env.addSource(function, sourceName, typeInformation);
    }

    protected DataStream<RowData> createInput(RichParallelSourceFunction<RowData> function, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        return env.addSource(function, sourceName, typeInformation);
    }

    protected DataStream<RowData> createInput(InputFormat<RowData, InputSplit> inputFormat) {
        return createInput(inputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /**
     * 初始化FlinkxCommonConf
     *
     * @param flinkxCommonConf
     */
    public void initFlinkxCommonConf(FlinkCommonConf flinkxCommonConf) {
        PropertiesUtil.initFlinkxCommonConf(flinkxCommonConf, this.codeConf);
        flinkxCommonConf.setCheckFormat(this.codeConf.getReader().getBooleanVal("check", true));
        SpeedConf speed = this.codeConf.getSpeed();
        flinkxCommonConf.setParallelism(speed.getReaderChannel() == -1 ? speed.getChannel() : speed.getReaderChannel());
    }
}
