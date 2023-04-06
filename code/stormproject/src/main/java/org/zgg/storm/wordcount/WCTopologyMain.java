package org.zgg.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WCTopologyMain {
    public static void main(String[] args) throws Exception {

        //1、准备一个TopologyBuilder
        //storm框架支持多语言，在Java环境下创建一个拓扑，需要使用TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

//        String DATASOURCE_SPOUT = MySpout.class.getSimpleName();
//        String SPLIT_BOLD = MySplitBolt.class.getSimpleName();
//        String COUNT_BOLT = MyCountBolt.class.getSimpleName();

        //MySpout类，在已知的英文句子中，所及发送一条句子出去。。。
        //这里的2指的是并发量，即storm中有10个线程并发执行。
        topologyBuilder.setSpout("mySpout", new ReadSpout(), 2); //配置并行度
        //MySplitBolt类，主要是将一行一行的文本内容切割成单词
        topologyBuilder.setBolt("mybolt1", new SplitBolt(), 2).shuffleGrouping("mySpout");

//        topologyBuilder.setBolt("mybolt1", new MySplitBolt(), 2)
//                .setNumTasks(4)  //配置并行度
//                .shuffleGrouping("mySpout");

        //MyCountBolt类，负责对单词的频率进行累加
        topologyBuilder.setBolt("mybolt2", new CountBolt(), 4).fieldsGrouping("mybolt1", new Fields("word"));
        //2、创建一个configuration，用来指定当前topology 需要的worker的数量
        //启动topology的配置信息
        Config config = new Config();
        //定义你希望集群分配多少个工作进程给你来执行这个topology
        config.setNumWorkers(4);   //配置并行度
        //3、提交任务  -----两种模式 本地模式和集群模式
        //这里将拓扑名称写死了mywordcount,所以在集群上打包运行的时候，不用写拓扑名称了！也可用arg[0]

//         StormSubmitter stormSubmitter = new StormSubmitter();
//         stormSubmitter.submitTopology(WordCountTopologyMain.class.getSimpleName(), config, topologyBuilder.createTopology());
//         stormSubmitter.submitTopology("test", config, topologyBuilder.createTopology());

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(WCTopologyMain.class.getSimpleName(),config,topologyBuilder.createTopology());
//        Thread.sleep(1000);
//        localCluster.shutdown();
    }

}
