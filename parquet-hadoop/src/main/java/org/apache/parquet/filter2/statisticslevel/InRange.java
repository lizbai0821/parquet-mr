package org.apache.parquet.filter2.statisticslevel;

/**
 * Created by bairan on 1/11/2016.
 */
public class InRange
{
    public String ColumnName;
    public Integer Lower = null;
    public Integer Upper = null;

    public InRange (String columnname, Integer lower, Integer upper){
        this.ColumnName = columnname;
        this.Lower = lower;
        this.Upper = upper;
    }

    public InRange (InRange other){
        this.ColumnName = other.ColumnName;
        this.Lower = other.Lower;
        this.Upper = other.Upper;
    }
    public boolean Match (InRange other){
        if (this.ColumnName.equals(other.ColumnName)){
            if(this.Lower == null && other.Lower!=null)
                return true;
        }
        return false;
    }

}
