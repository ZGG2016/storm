//package org.zgg.storm.hdfs;
//
//import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
//import org.apache.storm.hdfs.bolt.HdfsBolt;
//import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
//import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
//import org.apache.storm.hdfs.bolt.format.FileNameFormat;
//import org.apache.storm.hdfs.bolt.format.RecordFormat;
//import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
//import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
//import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
//import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.tuple.Fields;
//  //报错：Caused by: java.util.zip.ZipException: error in opening zip file
//public class HdfsMain {
//    public static void main(String[] args) throws Exception {
//        TopologyBuilder topologyBuilder = new TopologyBuilder();
//
//        Config config = new Config();
//
//        RecordFormat format = new DelimitedRecordFormat()
//                .withFieldDelimiter(" ");
//        SyncPolicy syncPolicy = new CountSyncPolicy(2);
//        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
//        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
//                .withPath("/foo");
//        HdfsBolt bolt = new HdfsBolt()
//                .withFsUrl("hdfs://192.168.14.44:9000")
//                .withFileNameFormat(fileNameFormat)
//                .withRecordFormat(format)
//                .withRotationPolicy(rotationPolicy)
//                .withSyncPolicy(syncPolicy);
//
//        topologyBuilder.setSpout("mySpout", new HdfsSpout(), 1);
//        topologyBuilder.setBolt("mybolt",bolt,1).fieldsGrouping("mySpout",new Fields("sentence"));
//
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology(HdfsMain.class.getSimpleName(),config,topologyBuilder.createTopology());
//    }
//}
