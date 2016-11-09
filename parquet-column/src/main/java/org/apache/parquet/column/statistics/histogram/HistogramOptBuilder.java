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
package org.apache.parquet.column.statistics.histogram;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kaiser on 16/10/20.
 */
public class HistogramOptBuilder {
    private String colNames = "";
    private String minValues = "";
    private String maxValues = "";
    private String bucketsCounts = "";

    public HistogramOptBuilder enableCols(String colNames) {
        this.colNames = colNames;
        return this;
    }

    public HistogramOptBuilder setMinValues(String minValues) {
        this.minValues = minValues;
        return this;
    }

    public HistogramOptBuilder setMaxValues(String maxValues) {
        this.maxValues = maxValues;
        return this;
    }

    public HistogramOptBuilder setBucketsCounts(String bucketsCounts) {
        this.bucketsCounts = bucketsCounts;
        return this;
    }


    public HistogramOpts build(MessageType messageType) {

        if (colNames.isEmpty()) {
            return null;
        }
        String[] cols = colNames.split(",");
        String[] minList = minValues.split(",");
        String[] maxList = maxValues.split(",");
        String[] countList = bucketsCounts.split(",");




        Map<ColumnDescriptor, HistogramOpts.HistogramEntry> columnDescriptorMap = new HashMap<>();

        for (int i = 0; i < cols.length; i++) {
            ColumnDescriptor columnDescriptor = messageType.getColumnDescription(new String[]{cols[i]});//?

            columnDescriptorMap.put(columnDescriptor,
                    new HistogramOpts.HistogramEntry((long) Double.parseDouble(minList[i]), (long) Double.parseDouble(maxList[i]), Integer.parseInt(countList[i])));
        }

        return new HistogramOpts(columnDescriptorMap);
    }
}
