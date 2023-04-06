//package org.zgg.storm.trident_sale;
//
//import org.apache.hadoop.hbase.client.Durability;
//import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
//import org.apache.storm.LocalDRPC;
//import org.apache.storm.StormSubmitter;
//import org.apache.storm.generated.StormTopology;
//import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
//import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
//import org.apache.storm.hbase.trident.mapper.TridentHBaseMapMapper;
//import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
//import org.apache.storm.hbase.trident.state.HBaseMapState;
//import org.apache.storm.hbase.trident.state.HBaseState;
//import org.apache.storm.hbase.trident.state.HBaseStateFactory;
//import org.apache.storm.hbase.trident.state.HBaseUpdater;
//import org.apache.storm.kafka.BrokerHosts;
//import org.apache.storm.kafka.StringScheme;
//import org.apache.storm.kafka.ZkHosts;
//import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
//import org.apache.storm.kafka.trident.TridentKafkaConfig;
//import org.apache.storm.spout.SchemeAsMultiScheme;
//import org.apache.storm.trident.Stream;
//import org.apache.storm.trident.TridentState;
//import org.apache.storm.trident.TridentTopology;
//import org.apache.storm.trident.operation.builtin.FilterNull;
//import org.apache.storm.trident.operation.builtin.FirstN;
//import org.apache.storm.trident.operation.builtin.MapGet;
//import org.apache.storm.trident.operation.builtin.Sum;
//import org.apache.storm.trident.state.StateFactory;
//import org.apache.storm.trident.state.StateType;
//import org.apache.storm.trident.testing.MemoryMapState;
//import org.apache.storm.tuple.Fields;
//
//import java.util.HashMap;
//import java.util.Map;
//
///*
//* 腾讯课堂：基于Storm流计算天猫双十一作战室项目实战（北风网）
//* kafka--storm--hbase--highchart
//*
//* 导入hbase有问题
//* */
//public class Top_N_Province {
//
//    private String zkUrl ;
//    private String brokerUrl ;
//    private String topic ;
//    private Fields order;
//
//    Top_N_Province(String zkUrl,String brokerUrl,String topic){
//        this.zkUrl = zkUrl;
//        this.brokerUrl = brokerUrl;
//        this.topic = topic;
//    }
//
//    //从kafka读数据
//    private TransactionalTridentKafkaSpout createKafkaSpout() {
//        BrokerHosts hosts = new ZkHosts(zkUrl);
//        TridentKafkaConfig config = new TridentKafkaConfig(hosts,topic);
//        //规定了从kafka消费的ByteBuffer如何转换成storm tuple，也控制这输出域的命名。
//        config.scheme = new SchemeAsMultiScheme(new StringScheme());
//        order = config.scheme.getOutputFields();  //指定输出域
//        return new TransactionalTridentKafkaSpout(config);
//    }
//
//    private TridentState addTridentState (TridentTopology tridentTopology){
//
//        TridentHBaseMapper tridentHBaseMapper = new SimpleTridentHBaseMapper()
//                .withColumnFamily("result")
//                .withColumnFields(new Fields("province_id","sum"))
//                .withRowKeyField("province_id");
//
//        HBaseState.Options options = new HBaseState.Options()
//                .withMapper(tridentHBaseMapper)
//                .withTableName("sale")
//                .withDurability(Durability.SYNC_WAL);
//
//        StateFactory hBaseStateFactory = new HBaseStateFactory(options);
//
//        return tridentTopology.newStream("spout1",createKafkaSpout())
//                .parallelismHint(2)
//                .each(order,new MySplit("\t"),new Fields("province_id","amt"))
//                .groupBy(new Fields("province_id"))
//                //.persistentAggregate(new MemoryMapState.Factory(),new Fields("amt"),new Sum(),new Fields("sum"));
//                .persistentAggregate(hBaseStateFactory,new Fields("amt"),new Sum(),new Fields("sum"));
//    }
//
//    private Stream addDRPCStream(TridentTopology tridentTopology, TridentState state, LocalDRPC drpc){
//
//        return tridentTopology.newDRPCStream("getProvinceSales",drpc)
//                .each(new Fields("args"),new MySplit(" "),new Fields("province_id"))
//                .groupBy(new Fields("province_id"))
//                .stateQuery(state,new Fields("province_id"),new MapGet(),new Fields("sum"))
//                .each(new Fields("sum"),new FilterNull())
//                .project(new Fields("province_id","sum"))
//                .applyAssembly(new FirstN(5,"sum",true)); //前5个，按sum分组，倒序
//
//    }
//
//    public StormTopology buildConsumerTopology(LocalDRPC drpc){
//        TridentTopology tridentTopology = new TridentTopology();
//        addDRPCStream(tridentTopology,addTridentState(tridentTopology),drpc);
//        return tridentTopology.build();
//    }
//
//    public Config getConsumerConfig() {
//        Config conf = new Config();
//        conf.setMaxSpoutPending(20);
//        return conf;
//    }
//
//    public static void main(String[] args) throws Exception {
//
//         String zkUrl = "datanode1:2181,datanode2:2181,datanode3:2181";
//         String brokerUrl = "datanode1:9092,datanode2:9092,datanode3:9092";
//         String topic = "protopic";
//
//        Top_N_Province top_n_province = new Top_N_Province(zkUrl,brokerUrl,topic);
//
//        Map<String, Object> hbConf = new HashMap<String, Object>();
//        hbConf.put("hbase.rootdir","hdfs://192.168.14.44:9000/user/hbase");
//
//        Config conf = new Config();
//        conf.put("hbase.conf", hbConf);
//        conf.setDebug(true);
//        conf.setMaxSpoutPending(20);
//        if(args.length == 3){
//            conf.setNumWorkers(3);
//            StormSubmitter.submitTopology(args[2],conf,top_n_province.buildConsumerTopology(null));
//        } else {
//            LocalDRPC drpc = new LocalDRPC();
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology(Top_N_Province.class.getSimpleName(),top_n_province.getConsumerConfig(),top_n_province.buildConsumerTopology(drpc));
//            for(int i=0;i<100;i++){
//                //System.out.println("DRPC RESULT: "+drpc.execute("getProvinceSales","1 2 3 4"));
//                drpc.execute("getProvinceSales","1 2 3 4");
//                Thread.sleep(1000);
//            }
//        }
//    }
//
//}
