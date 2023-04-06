package org.zgg.storm.hive;
//
//import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
//import org.apache.storm.hive.bolt.HiveBolt;
//import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
//import org.apache.storm.hive.common.HiveOptions;
//import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.tuple.Fields;
///*
//* 有问题
//* */
//public class HiveTopology {
//    public static void main(String[] args) throws Exception {
//
//        String metaStoreURI = "jdbc:hive2://192.168.0.6:10000/hive";
//        String database = "userdb";
//        String tablename = "test";
//
//
//        TopologyBuilder topologyBuilder = new TopologyBuilder();
//
//
//        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
//                .withColumnFields(new Fields("id","name","age"));
//        HiveOptions hiveOptions = new HiveOptions(metaStoreURI,database,tablename,mapper);
//        HiveBolt hiveBolt = new HiveBolt(hiveOptions);
//
//
//        topologyBuilder.setSpout("mySpout", new HiveSpout(), 2);
//        topologyBuilder.setBolt("mybolt",new MyHiveBolt(),2).shuffleGrouping("mySpout");
//        topologyBuilder.setBolt("mybolt2",hiveBolt,4).fieldsGrouping("mybolt", new Fields("id","name","age"));
//
//        Config config = new Config();
//
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology(HiveTopology.class.getSimpleName(), config, topologyBuilder.createTopology());
////        Thread.sleep(1000);
////        localCluster.shutdown();
//
//    }
//
//}
