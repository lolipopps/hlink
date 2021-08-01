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

package com.hlink.sink;

import com.hlink.conf.CodeConf;
import com.hlink.conf.FieldConf;
import com.hlink.conf.FlinkCommonConf;
import com.hlink.conf.SpeedConf;
import com.hlink.converter.RawTypeConvertible;
import com.hlink.sink.output.DtOutputFormatSinkFunction;
import com.hlink.utils.PropertiesUtil;
import com.hlink.utils.TableUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Abstract specification of Writer Plugin
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public abstract class SinkFactory implements RawTypeConvertible {

    protected CodeConf codeConf;
    protected TypeInformation<RowData> typeInformation;
    protected boolean useAbstractBaseColumn = true;

    public SinkFactory(CodeConf codeConf) {
        // 脏数据记录reader中的字段信息
        List<FieldConf> fieldList = codeConf.getWriter().getFieldList();
        if (CollectionUtils.isNotEmpty(fieldList)) {
            codeConf.getDirty().setReaderColumnNameList(codeConf.getWriter().getFieldNameList());
        }
        this.codeConf = codeConf;

        if (codeConf.getTransformer() == null || StringUtils.isBlank(codeConf.getTransformer().getTransformSql())) {
            typeInformation = TableUtil.getTypeInformation(Collections.emptyList(), getRawTypeConverter());
        } else {
            typeInformation = TableUtil.getTypeInformation(fieldList, getRawTypeConverter());
            useAbstractBaseColumn = false;
        }
    }

    /**
     * Build the write data flow with read data flow
     *
     * @param dataSet read data flow
     * @return write data flow
     */
    public abstract DataStreamSink<RowData> createSink(DataStream<RowData> dataSet);

    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);
        Preconditions.checkNotNull(outputFormat);

        DtOutputFormatSinkFunction<RowData> sinkFunction = new DtOutputFormatSinkFunction<>(outputFormat);
        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(sinkFunction);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    /**
     * 初始化FlinkxCommonConf
     *
     * @param flinkCommonConf
     */
    public void initFlinkxCommonConf(FlinkCommonConf flinkCommonConf) {
        PropertiesUtil.initFlinkxCommonConf(flinkCommonConf, this.codeConf);
        flinkCommonConf.setCheckFormat(this.codeConf.getWriter().getBooleanVal("check", true));
        SpeedConf speed = this.codeConf.getSpeed();
        flinkCommonConf.setParallelism(speed.getWriterChannel() == -1 ? speed.getChannel() : speed.getWriterChannel());
    }
}
