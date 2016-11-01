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

/**
 * kaiser ding Histogram
 */
public class Histogram {

    protected long[] buckets;
    protected int bucketsCount;
    protected long min;
    protected long max;


    protected int[] counters;

    public Histogram(HistogramOpts.HistogramEntry entry) {
        this.bucketsCount = entry.getBucketCount();
        this.min = entry.getMin();
        this.max = entry.getMax();
        this.buckets = entry.getEvenBuckets();
        this.counters = new int[bucketsCount];
    }


    public void merge(Histogram that) {
        if (this != that && this.counters.length == that.counters.length) {
            for (int i = 0; i < this.counters.length; i++) {
                this.counters[i] += that.counters[i];
            }
        } else {
            throw new IllegalArgumentException("Histograms are not compatible for merging." +
                    " this - " + this.toString() + " that - " + that.toString());
        }
    }


    public void addValue(long value) {
        //O(n)
        fastBucketAddFunction(value);
    }

    public void fastBucketAddFunction(long value) {
        if (value >= min && value <= max) {
            int bucketNumber = (int) (((value - min) / (max - min)) * bucketsCount);
            bucketNumber = Math.min(bucketNumber, buckets.length - 1);
            counters[bucketNumber]++;
        }
    }


    public void addLong(long value) {
        addValue(value);
    }

    public void addInteger(int value) {
        addLong(value);
    }

    public void addDouble(double value) {
        addLong(Double.doubleToLongBits(value));
    }

    public void addFloat(float value) {
        addLong(Float.floatToIntBits(value));
    }

    //where is test ??
}

