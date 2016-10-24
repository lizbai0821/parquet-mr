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

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the otions required for constructing a bloom filter including the number of bits and the number of hash functions.
 */
public class HistogramOpts {

    Map<ColumnDescriptor, HistogramEntry> filterEntryList = new HashMap<>();

    public static class HistogramEntry {

        private long max;
        private long min;
        private int bucketCount;
        private long[] evenBuckets;

        public HistogramEntry(long max, long min, int count) {
            this.max = max;
            this.min = min;
            this.bucketCount = count;

            evenBuckets = new long[count + 1];
            long span = (max - min) / count;
            for (int i = 0; i <= count; i++) {
                evenBuckets[i] = min + i * span;
            }
        }

        public long getMax() {
            return max;
        }

        public long getMin() {
            return min;
        }

        public int getBucketCount() {
            return bucketCount;
        }

        public long[] getEvenBuckets() {
            return evenBuckets;
        }
    }

    public HistogramOpts(Map<ColumnDescriptor, HistogramEntry> filterEntryList) {
        this.filterEntryList = filterEntryList;
    }

    public Map<ColumnDescriptor, HistogramEntry> getFilterEntryList() {
        return filterEntryList;
    }
}