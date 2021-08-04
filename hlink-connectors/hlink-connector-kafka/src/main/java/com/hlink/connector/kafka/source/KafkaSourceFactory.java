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

package com.hlink.connector.kafka.source;

 
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hlink.conf.CodeConf;
import com.hlink.connector.kafka.adapter.StartupModeAdapter;
import com.hlink.connector.kafka.conf.KafkaConf;
import com.hlink.connector.kafka.converter.KafkaColumnConverter;
import com.hlink.connector.kafka.enums.StartupMode;
import com.hlink.connector.kafka.serialization.RowDeserializationSchema;
import com.hlink.connector.kafka.util.KafkaUtil;
import com.hlink.converter.RawTypeConverter;
import com.hlink.source.SourceFactory;
import com.hlink.utils.GsonUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.common.requests.IsolationLevel;

import java.io.Serializable;
import java.util.Properties;

/**
 * Date: 2019/11/21
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaSourceFactory extends SourceFactory {

    /** kafka conf */
    protected KafkaConf kafkaConf;

    public KafkaSourceFactory(CodeConf config, StreamExecutionEnvironment env) {
        super(config, env);
        Gson gson = new GsonBuilder().registerTypeAdapter(StartupMode.class, new StartupModeAdapter()).create();
        GsonUtil.setTypeAdapter(gson);
        kafkaConf = gson.fromJson(gson.toJson(config.getReader().getParameter()), KafkaConf.class);
        super.initFlinkxCommonConf(kafkaConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("kafka not support transform");
        }
        Properties props = new Properties();
        props.put("group.id", kafkaConf.getGroupId());
        props.putAll(kafkaConf.getConsumerSettings());
        KafkaConsumer consumer = new KafkaConsumer(
                Lists.newArrayList(kafkaConf.getTopic()),
                new RowDeserializationSchema(
                        new KafkaColumnConverter(kafkaConf),
                        (Calculate & Serializable) (subscriptionState, tp) ->
                                subscriptionState.partitionLag(
                                        tp, IsolationLevel.READ_UNCOMMITTED)),
                props);
        switch (kafkaConf.getMode()) {
            case EARLIEST:
                consumer.setStartFromEarliest();
                break;
            case LATEST:
                consumer.setStartFromLatest();
                break;
            case TIMESTAMP:
                consumer.setStartFromTimestamp(kafkaConf.getTimestamp());
                break;
            case SPECIFIC_OFFSETS:
                consumer.setStartFromSpecificOffsets(KafkaUtil.parseSpecificOffsetsString(
                        kafkaConf.getTopic(),
                        kafkaConf.getOffset()));
                break;
            default:
                consumer.setStartFromGroupOffsets();
                break;
        }
        consumer.setCommitOffsetsOnCheckpoints(kafkaConf.getGroupId() != null);
        return createInput(consumer, codeConf.getReader().getName());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
