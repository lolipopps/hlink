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
package com.hlink.connector.console.converter;


import com.github.jsonzou.jmockdata.JMockData;
import com.hlink.converter.AbstractRowConverter;
import com.hlink.converter.IDeserializationConverter;
import com.hlink.converter.ISerializationConverter;
import com.hlink.record.AbstractBaseColumn;
import com.hlink.record.ColumnRowData;
import com.hlink.record.column.BigDecimalColumn;
import com.hlink.record.column.BooleanColumn;
import com.hlink.record.column.StringColumn;
import com.hlink.record.column.TimestampColumn;
import org.apache.flink.table.data.RowData;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public class StreamColumnConverter extends AbstractRowConverter<RowData, RowData, RowData, String> {

    private static final long serialVersionUID = 1L;
    private static final AtomicLong id = new AtomicLong(0L);

    public StreamColumnConverter(List<String> typeList) {
        super(typeList.size());
        for (int i = 0; i < typeList.size(); i++) {
            toInternalConverters[i] = wrapIntoNullableInternalConverter(createInternalConverter(typeList.get(i)));
            toExternalConverters[i] = wrapIntoNullableExternalConverter(createExternalConverter(typeList.get(i)), typeList.get(i));
        }
    }

    public StreamColumnConverter() {}

    @Override
    protected ISerializationConverter<ColumnRowData> wrapIntoNullableExternalConverter(ISerializationConverter serializationConverter, String type) {
        return (val, index, rowData) -> rowData.addField(((ColumnRowData) val).getField(index));
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "ID":
                return val -> new BigDecimalColumn(new BigDecimal(id.incrementAndGet()));
            case "INT":
            case "INTEGER":
                return val -> new BigDecimalColumn(JMockData.mock(int.class));
            case "BOOLEAN":
                return val -> new BooleanColumn(JMockData.mock(boolean.class));
            case "TINYINT":
                return val -> new BigDecimalColumn(JMockData.mock(byte.class));
            case "CHAR":
            case "CHARACTER":
                return val -> new StringColumn(JMockData.mock(char.class).toString());
            case "SHORT":
            case "SMALLINT":
                return val -> new BigDecimalColumn(JMockData.mock(short.class));
            case "LONG":
            case "BIGINT":
                return val -> new BigDecimalColumn(JMockData.mock(long.class));
            case "FLOAT":
                return val -> new BigDecimalColumn(JMockData.mock(float.class));
            case "DOUBLE":
                return val -> new BigDecimalColumn(JMockData.mock(double.class));
            case "DECIMAL":
                return val -> new BigDecimalColumn(JMockData.mock(BigDecimal.class));
            case "DATE":
            case "DATETIME":
            case "TIMESTAMP":
                return val -> new TimestampColumn(System.currentTimeMillis());
            case "TIME":
                return val -> new TimestampColumn((LocalTime.now().toNanoOfDay() / 1_000_000L));
            default:
                return val -> new StringColumn(JMockData.mock(String.class));
        }
    }

    @Override
    protected ISerializationConverter<ColumnRowData> createExternalConverter(String type) {
        return (val, index, rowData) -> rowData.addField(((ColumnRowData) val).getField(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData rowData) throws Exception {
        ColumnRowData data = new ColumnRowData(toInternalConverters.length);
        for (int i = 0; i < toInternalConverters.length; i++) {
            data.addField((AbstractBaseColumn) toInternalConverters[i].deserialize(data));
        }
        return data;
    }

    @Override
    public RowData toExternal(RowData rowData, RowData output) {
        return rowData;
    }
}
