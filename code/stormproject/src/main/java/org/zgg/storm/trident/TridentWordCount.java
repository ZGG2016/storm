package org.zgg.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentWordCount {
    public static class Split extends BaseFunction{

        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {

        //这个是一个batch Spout  一次发3个..
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));

        spout.setCycle(true);//Spout是否循环发送

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16)//类似于setSpout
                .each(new Fields("sentence"),new Split(), new Fields("word"))//setbolt  划分
                .groupBy(new Fields("word"))  //分组
                .persistentAggregate(new MemoryMapState.Factory(),new Count(), new Fields("count"))  //更新状态
                .parallelismHint(16);

        topology.newDRPCStream("words")  //将这个函数命名为 "words" .
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))  //去重
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))  //使用了 MapGet 函数来获取每个单词的出现个数
                .each(new Fields("count"),new FilterNull()) //用 FilterNull 这个过滤器将没有 count 结果的 words 过滤掉
                //使用 Sum 这个聚合器将这些 count 累加起来得到结果，汇总"cat the dog jumped"单词出现的总次数
               // .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
                .project(new Fields("word", "count"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);  //在spout task上能被处理的最多元组数量。该配置是应用于一个task上，不是spout or topologies
        //conf.setNumWorkers(3);
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TridentWordCount.class.getSimpleName(),conf,buildTopology(drpc));
        for(int i=0;i<100;i++){
            //指定函数名“words”，传入参数"cat the dog jumped"  ===》 DRPC调用流程
            System.out.println("DRPC RESULT: "+ drpc.execute("words", "cat the dog jumped"));
            Thread.sleep(1000);
        }

    }
}








