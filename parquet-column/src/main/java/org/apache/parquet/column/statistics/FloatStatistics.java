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

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilter;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterStatistics;
import org.apache.parquet.column.statistics.histogram.Histogram;
import org.apache.parquet.column.statistics.histogram.HistogramOpts;
import org.apache.parquet.column.statistics.histogram.HistogramStatistics;
import org.apache.parquet.column.values.bitpacking.Packer;

public class FloatStatistics extends Statistics<Float> implements BloomFilterStatistics<Float>, HistogramStatistics<Float> {

    private float max;
    private float min;
    private BloomFilter bloomFilter;
    private boolean isBloomFilterEnabled = false;
    private Histogram histogram;
    private boolean isHistogramEnabled = false;


    public FloatStatistics(ColumnStatisticsOpts columnStatisticsOpts) {
        super();
        if (columnStatisticsOpts != null) {

            BloomFilterOpts.BloomFilterEntry bloomFilterEntry = columnStatisticsOpts.getBloomFilterOpts();
            HistogramOpts.HistogramEntry histogramEntry = columnStatisticsOpts.getHistogramOpts();

            if (bloomFilterEntry != null) {
                this.bloomFilter = new BloomFilter(bloomFilterEntry);
                this.isBloomFilterEnabled = true;
            }
            if (histogramEntry != null) {
                this.histogram = new Histogram(histogramEntry);
                this.isHistogramEnabled = true;
            }

        }
    }

//    private void updateBloomFilterOptions(BloomFilterOpts.BloomFilterEntry statisticsOpts) {
//        if (statisticsOpts != null) {
//            bloomFilter =
//                    new BloomFilter(statisticsOpts.getNumBits(), statisticsOpts.getNumHashFunctions());
//            isBloomFilterEnabled = true;
//        }
//    }

    @Override
    public void updateStats(float value) {
        if (!this.hasNonNullValue()) {
            initializeStats(value, value);
        } else {
            updateStats(value, value);
        }

        if (isBloomFilterEnabled) {
//            add(value);
            this.bloomFilter.addFloat(value);
        }

        if (isHistogramEnabled) {
            this.histogram.addFloat(value);
        }
    }

    @Override
    public void mergeStatistics(Statistics stats) {
        super.mergeStatistics(stats);
        mergeBloomFilters(stats);
        mergeHistogram(stats);
    }

    @Override
    public void mergeBloomFilters(Statistics stats) {
        if (isBloomFilterEnabled && stats instanceof BloomFilterStatistics) {
            this.bloomFilter.merge(((BloomFilterStatistics) stats).getBloomFilter());
        }
    }

    @Override
    public void mergeHistogram(Statistics stats) {
        if (isHistogramEnabled && stats instanceof HistogramStatistics) {
            this.histogram.merge(((HistogramStatistics) stats).getHistogram());
        }
    }

    @Override
    public void mergeStatisticsMinMax(Statistics stats) {
        FloatStatistics floatStats = (FloatStatistics) stats;
        if (!this.hasNonNullValue()) {
            initializeStats(floatStats.getMin(), floatStats.getMax());
        } else {
            updateStats(floatStats.getMin(), floatStats.getMax());
        }
    }

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = Float.intBitsToFloat(BytesUtils.bytesToInt(maxBytes));
        min = Float.intBitsToFloat(BytesUtils.bytesToInt(minBytes));
        this.markAsNotEmpty();
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.intToBytes(Float.floatToIntBits(max));
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.intToBytes(Float.floatToIntBits(min));
    }

    @Override
    public boolean isSmallerThan(long size) {
        return !hasNonNullValue() || (8 < size);
    }

    @Override
    public String toString() {
        if (this.hasNonNullValue())
            return String.format("min: %.5f, max: %.5f, num_nulls: %d", min, max, this.getNumNulls());
        else if (!this.isEmpty())
            return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
        else
            return "no stats for this column";
    }

    public void updateStats(float min_value, float max_value) {
        if (min_value < min) {
            min = min_value;
        }
        if (max_value > max) {
            max = max_value;
        }
    }

    public void initializeStats(float min_value, float max_value) {
        min = min_value;
        max = max_value;
        this.markAsNotEmpty();
    }

    @Override
    public Float genericGetMin() {
        return min;
    }

    @Override
    public Float genericGetMax() {
        return max;
    }

    public float getMax() {
        return max;
    }

    public float getMin() {
        return min;
    }

    public void setMinMax(float min, float max) {
        this.max = max;
        this.min = min;
        this.markAsNotEmpty();
    }

    @Override
    public void add(Float value) {
        if (isBloomFilterEnabled)
            bloomFilter.addFloat(value);
        if (isHistogramEnabled)
            histogram.addFloat(value);
    }

    @Override
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public Histogram getHistogram() {
        return histogram;
    }

    @Override
    public boolean test(Float value) {
        return bloomFilter.testFloat(value);
    }

    @Override
    public boolean test(Long value1, Long value2) { return histogram.testLong(value1, value2);}

    @Override
    public Long Quality(Long value1, Long value2) { return histogram.QualityLong(value1, value2);}

    @Override
    public boolean isBloomFilterEnabled() {
        return isBloomFilterEnabled;
    }

    @Override
    public boolean isHistogramEnabled() {
        return isHistogramEnabled;
    }
}
