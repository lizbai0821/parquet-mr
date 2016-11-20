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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * kaiser ding Histogram
 */
public class Histogram implements Serializable{

    protected long min; // lower bound
    protected long max; // upper bound
    protected int bucketsCount; // # of buckets
    protected long[] buckets; // bound list of buckets
    protected long[] counters; // # of values in each bucket

    public Histogram(HistogramOpts.HistogramEntry entry) {
        this.bucketsCount = entry.getBucketCount();
        this.min = entry.getMin();
        this.max = entry.getMax();
        this.buckets = entry.getEvenBuckets();
        this.counters = new long[bucketsCount];
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

    public void setCounters(long[] counters_) {
        this.counters = counters_.clone();
    }

    public void setBuckets(long[] buckets_) {
        this.buckets = buckets_.clone();
    }


    public void addValue(long value) {
        //O(n)
        fastBucketAddFunction(value);
    }

    public void fastBucketAddFunction(long value) {
        if (value >= min && value <= max) {
            int bucketNumber = (int) (((double)(value - min) / (max - min)) * bucketsCount); // double is needed?
            bucketNumber = Math.min(bucketNumber, counters.length - 1);
            counters[bucketNumber]++;
        }
    }

    public long getmin(){return min;}
    public long getmax(){return max;}
    public int getbucketsCount() {return bucketsCount;}
    public long[] getbuckets() {return buckets;}
    public long[] getcounters() {return counters;}


    public void addLong(long value) {
        addValue(value);
    }

    public void addInteger(int value) {
        addLong(value);
    }

    public void addDouble(double value) {
        addLong((long)value);
    }

    public void addFloat(float value) {
        addInteger((int)value);
    }

    public boolean testLong (long low, long up) {
        if(low<min || up>max)
            return true;
        int bucket_low = (int) (((double)(low - min) / (max - min)) * bucketsCount);
        bucket_low = Math.min(bucket_low, counters.length - 1);

        int bucket_up = (int) (((double)(up - min) / (max - min)) * bucketsCount);
        bucket_up = Math.min(bucket_up, counters.length - 1);

        while(bucket_low <= bucket_up){
            if(counters[bucket_low] != 0)
                return true;  // buckets not empty
            bucket_low++;
        }
        return false;
    }

    public boolean testFloat(float low, float up){
        return testInteger((int)low, (int)up);
    }

    public boolean testInteger (int low, int up){
        return testLong(low, up);
    }

    public boolean testDouble (double low, double up){
        return testLong((long)low, (long)up);
    }


    public Long QualityLong (long low, long up) {
        if(low>max || up<min)
            return 0L;
        double bucket_low_f = (((double)(low - min) / (max - min)) * bucketsCount);
        bucket_low_f = Math.max(bucket_low_f, 0);

        int bucket_low = (int) bucket_low_f+1;

        double bucket_up_f = (((double)(up - min) / (max - min)) * bucketsCount);
        bucket_up_f = Math.min(bucket_up_f, counters.length - 1);

        int bucket_up = (int) bucket_up_f;

        Long result = 0L;
        while(bucket_low < bucket_up){
            result+= counters[bucket_low];
            bucket_low++;
        }

        if(bucket_low-1 == bucket_up){
            result+= (int)(counters[bucket_up]*(bucket_up_f-bucket_low_f));
        }
        else {
            result = result + (int)(counters[bucket_low-1]*((float)bucket_low-bucket_low_f)) + (int)(counters[bucket_up]*(bucket_up_f-(float)bucket_up));
        }
        return result;
    }

    public Long QualityFloat(float low, float up){
        return QualityInteger((int)low, (int)up);
    }

    public Long QualityInteger (int low, int up){
        return QualityLong(low, up);
    }

    public Long QualityDouble (double low, double up){
        return QualityLong((long)low, (long)up);
    }
}

