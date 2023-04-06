package org.zgg.storm.hbase;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

public class WordCounter implements IBasicBolt {
    public void prepare(Map stormConf, TopologyContext context) {

    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(tuple(input.getValues().get(0), 1));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
