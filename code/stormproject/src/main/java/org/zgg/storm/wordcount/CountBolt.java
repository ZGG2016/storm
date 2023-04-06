package org.zgg.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    private OutputCollector collector;
    //用来保存最后计算的结果key=单词，value=单词个数
    private Map<String, Integer> map = new HashMap<String, Integer>();


    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer num = input.getInteger(1);
        // System.out.println(Thread.currentThread().getId() + "    word:" + word);
        if (map.containsKey(word)) {
            Integer count = map.get(word);
            map.put(word, count + num);
        } else {
            map.put(word, num);
        }
        //System.out.println("count:" + map);
        System.out.println(word+":"+map.get(word));
        //this.collector.emit(new Values(word,map.get(word)));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        //declarer.declare(new Fields("word", "num"));
    }
}
