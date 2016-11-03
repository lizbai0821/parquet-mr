package org.apache.parquet.filter2.statisticslevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        List<String> columnNameList = new ArrayList<>();
        //find all gt
        int index = 0;
        while (index < planText.length()) {
            int start = planText.indexOf("gt(", index);
            if (start < 0)
                return;
            int end = planText.indexOf(")", start);
            String[] columnAndValue = planText.substring(start + 3, end - 1).split(",");
            String columnName = columnAndValue[0];
            Long greaterValue = Long.valueOf(columnAndValue[1]);
            columnNameList.add(columnName);

            if (!columnRangeMap.containsKey(columnName)) {
                InRange range = new InRange(columnName);
                range.setLower(greaterValue);
            } else {

                InRange inRange = columnRangeMap.get(columnName);
                if (greaterValue > inRange.getLower()) {
                    inRange.setLower(greaterValue);
                    columnRangeMap.put(columnName, inRange);
                }
            }
            index = end;
        }

        //mathch lt
        for (String columnName : columnNameList) {
            int start = planText.indexOf("lt(" + columnName);
            if (start < 0) {
                columnRangeMap.remove(columnName);
            } else {
                int end = planText.indexOf(")", start);
                Long lessValue = Long.valueOf(planText.substring(start + 3 + columnName.length() + 1, end - 1));
                InRange inRange = columnRangeMap.get(columnName);
                if (lessValue < inRange.getUpper()) {
                    inRange.setUpper(lessValue);
                    columnRangeMap.put(columnName, inRange);
                }
            }
        }

    }



}
