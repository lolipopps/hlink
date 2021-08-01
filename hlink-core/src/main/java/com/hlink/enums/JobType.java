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

package com.hlink.enums;

import org.apache.commons.lang3.StringUtils;


public enum JobType {
    /**
     * sql job
     */
    SQL(0, "sql"),
    /**
     * stream job
     */
    CODE(1, "code");

    private int type;

    private String name;

    JobType(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public static JobType getByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("JobType name cannot be null or empty , just support sql or code jobType !!! ");
        }
        switch (name) {
            case "sql":
                return SQL;
            case "code":
                return CODE;
            default:
                throw new RuntimeException("just support sql or code jobType !!!");
        }
    }
}
