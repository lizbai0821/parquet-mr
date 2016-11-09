/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.filter2.statisticslevel;

/**
 * Created by bairan on 1/11/2016.
 */

public class InRange   // should be created as an abstract class?
{
    public String columnName;
    public Long lower = Long.MIN_VALUE;
    public Long upper = Long.MAX_VALUE;

    public InRange(String columnName) {
        this.columnName = columnName;
    }

    public Long getLower() {
        return lower;
    }

    public Long getUpper() {
        return upper;
    }

    public void setUpper(Long upper) {
        this.upper = upper;
    }

    public void setLower(Long lower) {
        this.lower = lower;
    }

}
