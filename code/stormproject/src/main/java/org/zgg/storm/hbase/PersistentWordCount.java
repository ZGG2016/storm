package org.zgg.storm.hbase;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
/*
*
* 测试storm和hbase的连接
*
* */
public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";

    public static void main(String[] args) throws Exception{
        Config config = new Config();
        //配置hbase
        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir","hdfs://192.168.14.44:9000/user/hbase");
        hbConf.put("hbase.zookeeper.quorum","192.168.14.44");
        config.put("hbase.conf", hbConf);


        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("test",mapper).withConfigKey("hbase.conf");
//  #################################################################################################
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT,new WordSpout(),1);
        builder.setBolt(COUNT_BOLT, new WordCounter(), 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(HBASE_BOLT, hbase, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));

        config.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(PersistentWordCount.class.getSimpleName(), config, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();
        System.exit(0);



    }
}
