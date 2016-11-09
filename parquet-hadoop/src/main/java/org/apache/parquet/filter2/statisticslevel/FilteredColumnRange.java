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

import java.util.*;

/**
 * Created by bairan on 1/11/2016.
 */
public class FilteredColumnRange {



    public Map<String, InRange> columnRangeMap = new HashMap<>();

    public FilteredColumnRange(String planText) {
        this.insertRange(planText);
    }

    public Map<String, InRange> getColumnRangeMap() {
        return columnRangeMap;
    }

    public void insertRange(String planText) {

        Set<String> columnNameSet = new HashSet<>();
        //find all gt
        int index = 0;
        while (index < planText.length()) {
            int start = planText.indexOf("gt(", index);
            if (start < 0)
                break;
            int end = planText.indexOf(")", start);
            String[] columnAndValue = planText.substring(start + 3, end).split(", ");
            String columnName = columnAndValue[0];
            String columnValue = columnAndValue[1];
            long greaterValue = (long) (Double.parseDouble(columnValue));
            columnNameSet.add(columnName);

            InRange inRange = null;
            if (columnRangeMap.containsKey(columnName)) {
                inRange = columnRangeMap.get(columnName);
            } else {
                inRange = new InRange(columnName);
            }

            if (greaterValue > inRange.getLower()) {
                inRange.setLower(greaterValue);
                columnRangeMap.put(columnName, inRange);
            }
            index = end + 1;
        }

        //mathch lt
        for (String columnName : columnNameSet) {
            int start = planText.indexOf("lt(" + columnName);
            if (start < 0) {
                columnRangeMap.remove(columnName);
            } else {
                int end = planText.indexOf(")", start);
                long lessValue = (long) (Double.parseDouble(planText.substring(start + "lt(".length() + columnName.length() + ", ".length(), end)));
                InRange inRange = columnRangeMap.get(columnName);
                if (lessValue < inRange.getUpper()) {
                    inRange.setUpper(lessValue);
                    columnRangeMap.put(columnName, inRange);
                }
            }
        }

    }



}
