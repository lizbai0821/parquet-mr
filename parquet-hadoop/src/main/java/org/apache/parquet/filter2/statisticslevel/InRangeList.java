package org.apache.parquet.filter2.statisticslevel;

import com.sun.org.apache.regexp.internal.RE;

import java.util.ArrayList;

/**
 * Created by bairan on 1/11/2016.
 */
public class InRangeList {
    public ArrayList<InRange> InRangeList;
    public InRangeList(){this.InRangeList = new ArrayList<InRange>();}
    public InRangeList(String planText){
        this.InRangeList = new ArrayList<InRange>();
        this.InsertGt(planText);
        this.InsertLt(planText);
    }
    public InRangeList(ArrayList<InRange> list){this.InRangeList = list;}

    public ArrayList<InRange> getList(){return this.InRangeList;}

    public void InsertGt(String planText){
        int index = 0;
        while (index < planText.length()){
            int start = planText.indexOf("gt(",index);
            int end = planText.indexOf(")", start);
            String Sub = planText.substring(start+3, end-1);
            this.InRangeList.add(new InRange(Sub.split(",")[0], Long.valueOf((Sub.split(",")[1])), null));
            index = end;
        }
        return;
    }

    public void InsertLt(String planText){
        int index = 0;
        while (index < planText.length()){
            int start = planText.indexOf("lt(",index);
            int end = planText.indexOf(")", start);
            String Sub = planText.substring(start+3, end-1);
            InRangeList.add(new InRange(Sub.split(",")[0], null, Long.valueOf((Sub.split(",")[1]))));
            index = end;
        }
        return;
    }

    public void AddElement (InRange element){
        if(!this.InRangeList.contains(element)) // avoid duplications
            this.InRangeList.add(element);
        return;
    }

    public InRangeList SelfJoin(){
        InRangeList ResultList = new InRangeList();
        for (int i=0; i<this.InRangeList.size(); i++){
            for (int j=i+1; j<this .InRangeList.size(); j++){
                InRange Temp1 = new InRange(this.InRangeList.get(i));
                InRange Temp2 = new InRange(this.InRangeList.get(j));
                if(Temp1.Match(Temp2)){
                    InRange Element = new InRange(Temp1.ColumnName, Temp2.Lower, Temp1.Upper);
                    ResultList.AddElement(Element);
                }
            }
        }
        return ResultList;
    }
}
