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

package com.hlink.connector.console.table;


import com.google.common.collect.Lists;
import com.hlink.connector.console.conf.ConsoleConf;
import com.hlink.connector.console.sink.StreamDynamicTableSink;
import com.hlink.connector.console.source.StreamDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import java.util.HashSet;
import java.util.Set;

import static com.hlink.connector.console.options.StreamOptions.*;


public class StreamDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final String IDENTIFIER = "console";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(NUMBER_OF_ROWS);
        options.add(ROWS_PER_SECOND);
        options.add(PRINT);
        options.add(SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 1.?????????requiredOptions???optionalOptions??????
        final ReadableConfig config = helper.getOptions();

        // 2.????????????
        helper.validate();

        // 3.????????????
        ConsoleConf sinkConf = new ConsoleConf();
        sinkConf.setPrint(config.get(PRINT));
        sinkConf.setParallelism(config.get(SINK_PARALLELISM));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StreamDynamicTableSink(
                sinkConf,
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                physicalSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        ConsoleConf streamConf = new ConsoleConf();
        streamConf.setSliceRecordCount(Lists.newArrayList(options.get(NUMBER_OF_ROWS)));
        streamConf.setPermitsPerSecond(options.get(ROWS_PER_SECOND));

        return new StreamDynamicTableSource(schema, streamConf);
    }
}
