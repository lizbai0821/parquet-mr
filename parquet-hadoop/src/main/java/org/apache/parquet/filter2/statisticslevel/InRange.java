package org.apache.parquet.filter2.statisticslevel;

/**
 * Created by bairan on 1/11/2016.
 */

public class InRange   // should be created as an abstract class?
{
    public String columnName;
    public Long lower = Long.MAX_VALUE;
    public Long upper = Long.MIN_VALUE;

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
