package org.zgg.storm.hdfs;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class HdfsSpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    private FileReader fileReader;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        try {
            this.fileReader = new FileReader("C:\\Users\\ZGG\\Desktop\\test.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = collector;
    }

    public void nextTuple() {

        String str;
        BufferedReader reader = new BufferedReader(fileReader);

        try {
            while ((str = reader.readLine()) != null) {
                this.collector.emit(new Values(str));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
