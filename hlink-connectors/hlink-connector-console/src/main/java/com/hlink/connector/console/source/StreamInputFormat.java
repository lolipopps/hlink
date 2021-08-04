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

package com.hlink.connector.console.source;


import com.hlink.connector.console.conf.ConsoleConf;
import com.hlink.exception.ReadRecordException;
import com.hlink.source.input.BaseRichInputFormat;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.shaded.curator4.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.table.data.RowData;


@Data
@Slf4j
public class StreamInputFormat extends BaseRichInputFormat {
    private ConsoleConf streamConf;
    private RateLimiter rateLimiter;
    private long recordRead = 0;
    private long channelRecordNum;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i,minNumSplits);
        }

        return inputSplits;
    }

    @Override
    public void openInternal(InputSplit inputSplit) {
        if (CollectionUtils.isNotEmpty(streamConf.getSliceRecordCount())
                && streamConf.getSliceRecordCount().size() > inputSplit.getSplitNumber()) {
            channelRecordNum = streamConf.getSliceRecordCount().get(inputSplit.getSplitNumber());
        }
        if(streamConf.getPermitsPerSecond() > 0){
            rateLimiter = RateLimiter.create(streamConf.getPermitsPerSecond());
        }
        log.info("The record number of channel:[{}] is [{}]", inputSplit.getSplitNumber(), channelRecordNum);
    }

    @Override
    @SuppressWarnings("all")
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            rowData = rowConverter.toInternal(rowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        return rowData;
    }

    @Override
    public boolean reachedEnd() {
        return ++recordRead > channelRecordNum && channelRecordNum > 0;
    }

    @Override
    protected void closeInternal() {
        recordRead = 0;
    }

    public ConsoleConf getStreamConf() {
        return streamConf;
    }

    public void setStreamConf(ConsoleConf streamConf) {
        this.streamConf = streamConf;
    }
}