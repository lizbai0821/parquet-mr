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
