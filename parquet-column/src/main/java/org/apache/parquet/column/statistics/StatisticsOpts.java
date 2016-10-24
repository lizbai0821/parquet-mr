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
package org.apache.parquet.column.statistics;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilter;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
import org.apache.parquet.column.statistics.histogram.Histogram;
import org.apache.parquet.column.statistics.histogram.HistogramOptBuilder;
import org.apache.parquet.column.statistics.histogram.HistogramOpts;
import org.apache.parquet.filter2.predicate.Operators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StatisticsOpts {
    Map<ColumnDescriptor, ColumnStatisticsOpts> statisticsOptsMap = new HashMap<>();

    // Used for test
    public StatisticsOpts() {
    }

    public StatisticsOpts(BloomFilterOpts bloomFilterOpts) {
        if (bloomFilterOpts != null) {
            Map<ColumnDescriptor, BloomFilterOpts.BloomFilterEntry> bloomFilterEntryMap =
                    bloomFilterOpts.getFilterEntryList();
            for (ColumnDescriptor c : bloomFilterEntryMap.keySet()) {
                statisticsOptsMap.put(c, new ColumnStatisticsOpts(bloomFilterEntryMap.get(c)));
            }
        }
    }

    public StatisticsOpts(BloomFilterOpts bloomFilterOpts, HistogramOpts histogramOpts) {

        Map<ColumnDescriptor, BloomFilterOpts.BloomFilterEntry> bloomFilterEntryMap = null;
        Map<ColumnDescriptor, HistogramOpts.HistogramEntry> histogramEntryMap = null;
        Set<ColumnDescriptor> keySet = new HashSet<>();

        if (bloomFilterOpts != null) {
            bloomFilterEntryMap = bloomFilterOpts.getFilterEntryList();
            keySet.addAll(bloomFilterEntryMap.keySet());
        }
        if (histogramEntryMap != null) {
            histogramEntryMap = histogramOpts.getFilterEntryList();
            keySet.addAll(histogramEntryMap.keySet());
        }

        for (ColumnDescriptor cd : keySet) {
            BloomFilterOpts.BloomFilterEntry bloomFilterEntry = bloomFilterEntryMap == null ? null : bloomFilterEntryMap.get(cd);
            HistogramOpts.HistogramEntry histogramEntry = histogramEntryMap == null ? null : histogramEntryMap.get(cd);

            ColumnStatisticsOpts columnStatisticsOpts = new ColumnStatisticsOpts(bloomFilterEntry, histogramEntry);
            statisticsOptsMap.put(cd, columnStatisticsOpts);
        }


    }


    public ColumnStatisticsOpts getStatistics(ColumnDescriptor colDescriptor) {
        return statisticsOptsMap.get(colDescriptor);
    }
}
