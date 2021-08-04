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

package com.hlink.connector.console.source;

import com.hlink.conf.CodeConf;
import com.hlink.conf.FieldConf;
import com.hlink.connector.console.conf.ConsoleConf;
import com.hlink.connector.console.converter.StreamColumnConverter;
import com.hlink.connector.console.converter.StreamRawTypeConverter;
import com.hlink.connector.console.converter.StreamRowConverter;
import com.hlink.converter.AbstractRowConverter;
import com.hlink.converter.RawTypeConverter;
import com.hlink.source.SourceFactory;
import com.hlink.utils.GsonUtil;
import com.hlink.utils.TableUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamSourceFactory extends SourceFactory {
    private final ConsoleConf streamConf;

    public StreamSourceFactory(CodeConf config, StreamExecutionEnvironment env) {
        super(config, env);
        streamConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getReader().getParameter()), ConsoleConf.class);
        streamConf.setColumn(config.getReader().getFieldList());
        super.initFlinkxCommonConf(streamConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        StreamInputFormatBuilder builder = new StreamInputFormatBuilder();
        builder.setConsoleConf(streamConf);
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            List<String> typeList =
                    streamConf.getColumn().stream()
                            .map(FieldConf::getType)
                            .collect(Collectors.toList());
            rowConverter = new StreamColumnConverter(typeList);
        } else {
            final RowType rowType =
                    TableUtil.createRowType(streamConf.getColumn(), getRawTypeConverter());
            rowConverter = new StreamRowConverter(rowType);
        }

        builder.setRowConverter(rowConverter);

        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return StreamRawTypeConverter::apply;
    }
}
