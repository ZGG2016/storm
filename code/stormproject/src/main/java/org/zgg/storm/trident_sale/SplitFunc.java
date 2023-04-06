package org.zgg.storm.trident_sale;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
/*
* 腾讯课堂：基于Storm流计算天猫双十一作战室项目实战（北风网）
*
* */
public class SplitFunc extends BaseFunction {

    String splitBy = null;

    SplitFunc(String splitBy){
        this.splitBy = splitBy;
    }


    public void execute(TridentTuple tuple, TridentCollector collector) {
        String[] orderArr = tuple.getString(0).split(splitBy);
        //13    80.1    4  798
        // order_id,order_amt,area_id,user_id
        Double amt = Double.parseDouble(orderArr[1]);
        String province_id = orderArr[2];
        //System.err.println("************"+province_id+"************"+amt);
        collector.emit(new Values(province_id,amt));
    }
}
