package org.zgg.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.ReadOnlyState;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.ReadOnlyMapState;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.*;

public class TridentReach {
  //获取所有发布该URL的人
  public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
    put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
    put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
    put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
  }};

  //发布该URL的人的关注者
  public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
    put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
    put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
    put("tim", Collections.singletonList("alex"));
    put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
    put("adam", Arrays.asList("david", "carissa"));
    put("mike", Arrays.asList("john", "bob"));
    put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
  }};

  public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {

      Map _map;

      public StaticSingleKeyMapState(Map map) {
          _map = map;
      }

      public List<Object> multiGet(List<List<Object>> keys) {
          List<Object>  ret = new ArrayList();
          for(List<Object> key:keys){
              Object singleKey = key.get(0);
              ret.add(_map.get(singleKey));
          }
          return ret;
      }

      public static class Factory implements StateFactory {
          Map _map;

          public Factory(Map map) {
              _map = map;
          }

          public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
              return new StaticSingleKeyMapState(_map);
          }
      }

  public static class ExpandList extends BaseFunction {


      public void execute(TridentTuple tuple, TridentCollector collector) {
            List l = (List) tuple.getValue(0);
            if(l != null ){
                for(Object o : l){
                    collector.emit(new Values(o));
                }
            }
      }
      }
  }


  public static class One implements CombinerAggregator<Integer>{

      public Integer init(TridentTuple tuple) {
          return 1;
      }

      public Integer combine(Integer val1, Integer val2) {
          return 1;
      }

      public Integer zero() {
          return 1;
      }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
    TridentTopology topology = new TridentTopology();
    //读取数据
    TridentState urlToTweeters = topology.newStaticState(new StaticSingleKeyMapState.Factory(TWEETERS_DB));
    TridentState tweetersToFollowers = topology.newStaticState(new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));

    topology.newDRPCStream("reach", drpc)
            .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
            //使用ExpandList函数将该列表转换成一个个的Tuple分别发射出去。
            .each(new Fields("tweeters"), new StaticSingleKeyMapState.ExpandList(), new Fields("tweeter"))
            .shuffle()  //将需要处理的tweeter分配到集群的每一个分区
            .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
            .each(new Fields("followers"),new StaticSingleKeyMapState.ExpandList(), new Fields("follower"))
            .groupBy(new Fields("follower"))
            //使用One聚合器对每一个分组进行聚合，One聚合器仅仅是简单地为每一个分组发射一个Tuple（重复的Tuple被舍弃）
            .aggregate(new One(), new Fields("one"))
            .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
    return topology.build();

  }

  public static void main(String[] args) throws Exception {
    LocalDRPC drpc = new LocalDRPC();

    Config conf = new Config();
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology(TridentReach.class.getSimpleName(), conf, buildTopology(drpc));

    Thread.sleep(2000);

    System.out.println("REACH: " + drpc.execute("reach", "aaa"));
    System.out.println("REACH: " + drpc.execute("reach", "foo.com/blog/1"));
    System.out.println("REACH: " + drpc.execute("reach", "engineering.twitter.com/blog/5"));


    cluster.shutdown();
    drpc.shutdown();
  }
}
