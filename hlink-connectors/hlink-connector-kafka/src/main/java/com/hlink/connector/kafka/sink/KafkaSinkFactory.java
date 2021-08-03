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
package com.hlink.connector.kafka.sink;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hlink.conf.CodeConf;
import com.hlink.connector.kafka.adapter.StartupModeAdapter;
import com.hlink.connector.kafka.conf.KafkaConf;
import com.hlink.connector.kafka.converter.KafkaColumnConverter;
import com.hlink.connector.kafka.enums.StartupMode;
import com.hlink.connector.kafka.serialization.RowSerializationSchema;
import com.hlink.converter.RawTypeConverter;
import com.hlink.sink.SinkFactory;
import com.hlink.utils.GsonUtil;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/**
 * Date: 2021/04/07
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaSinkFactory extends SinkFactory {
    protected KafkaConf kafkaConf;

    public KafkaSinkFactory(CodeConf config) {
        super(config);
        Gson gson = new GsonBuilder().registerTypeAdapter(StartupMode.class, new StartupModeAdapter()).create();
        GsonUtil.setTypeAdapter(gson);
        kafkaConf = gson.fromJson(gson.toJson(config.getWriter().getParameter()), KafkaConf.class);
        super.initFlinkxCommonConf(kafkaConf);
    }

    @Override
    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    @Override
    protected DataStreamSink<RowData> createOutput(DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat, String sinkName) {
        Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);

        Properties props = new Properties();
        props.putAll(kafkaConf.getProducerSettings());

        KafkaProducer kafkaProducer = new KafkaProducer(
                kafkaConf.getTopic(),
                new RowSerializationSchema(kafkaConf.getTopic(), new KafkaColumnConverter(kafkaConf)),
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);

        DataStreamSink<RowData> dataStreamSink = dataSet.addSink(kafkaProducer);
        dataStreamSink.name(sinkName);

        return dataStreamSink;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("kafka not support transform");
        }
        return createOutput(dataSet, null);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
