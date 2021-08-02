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
package com.hlink.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.alibaba.fastjson.JSONObject;
import com.hlink.constants.ConstantValue;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toMap;

public class JsonUtil {

    public static String JsonValueReplace(String json, HashMap<String, String> parameter){
        for(String item: parameter.keySet()){
            if(json.contains("${"+item+"}")){
                json = json.replace("${"+item+"}", parameter.get(item));
            }
        }
        return json;
    }

    /**
     * 将命令行中的修改命令转化为HashMap保存
     */
    public static HashMap<String, String> CommandTransform(String command) {
        HashMap<String, String> parameter = new HashMap<>();
        String[] split = StringUtils.split(command, ConstantValue.COMMA_SYMBOL);
        for (String item : split) {
            String[] temp = item.split(ConstantValue.EQUAL_SYMBOL);
            parameter.put(temp[0], temp[1]);
        }
        return parameter;
    }

    /**
     * 获取表的元数据结构
     *
     * @return HashMap<String, HashMap < String, String>>
     */
    public static Map<String, String> loadJson(String dimFilePath) {
        try {
            String dim = FileUtil.readJsonFile(dimFilePath);
            JSONObject obj = JSONObject.parseObject(dim);
            HashMap<String, String> res = new HashMap<>();
            for (String key : obj.keySet()) {
                String record = obj.getJSONObject(key).toJSONString();
                res.put(key, record);
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取表的元数据结构
     *
     */
    public static Map<String, String> loadLineJson(String dimFilePath) {
        try {
            HashMap<String, String> res = new HashMap<>();
            List<String> lists = FileUtil.readFileByLine(dimFilePath);
            for(String li:lists) {
                String obj = JSONObject.parseObject(li).toJSONString();
                res.put(li, obj);

            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }




    public static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);


    /**
     * json反序列化成实体对象
     * @param jsonStr   json字符串
     * @param clazz     实体类class
     * @param <T>       泛型
     * @return          实体对象
     */
    public static <T> T toObject(String jsonStr, Class<T> clazz) {
        try {
            return objectMapper.readValue(jsonStr, clazz);
        }catch (IOException e){
            throw new RuntimeException("error parse [" + jsonStr + "] to [" + clazz.getName() + "]", e);
        }
    }

    /**
     * 实体对象转json字符串
     * @param obj 实体对象
     * @return  json字符串
     */
    public static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        }catch (JsonProcessingException e){
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转格式化输出的json字符串(用于日志打印)
     * @param obj 实体对象
     * @return  格式化输出的json字符串
     */
    public static String toPrintJson(Object obj) {
        try {
            Map<String, Object> result = objectMapper.readValue(objectMapper.writeValueAsString(obj), HashMap.class);
            MapUtil.replaceAllElement(result, Lists.newArrayList("pwd", "password"), "******");
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        }catch (Exception e){
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转格式化输出的json字符串(用于日志打印)
     * @param obj 实体对象
     * @return  格式化输出的json字符串
     */
    public static String toFormatJson(Object obj) {
        try {
            Map<String, String> collect = ((Properties) obj)
                    .entrySet()
                    .stream()
                    .collect(toMap(v -> v.getKey().toString(), v -> v.getValue().toString()));
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(collect);
        }catch (Exception e){
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }

    /**
     * 实体对象转byte数组
     * @param obj 实体对象
     * @return  byte数组
     */
    public static byte[] toBytes(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        }catch (JsonProcessingException e){
            throw new RuntimeException("error parse [" + obj + "] to json", e);
        }
    }
}
