//package org.zgg.storm.redis;
//
//import org.apache.storm.redis.common.config.JedisClusterConfig;
//import org.apache.storm.redis.common.config.JedisPoolConfig;
//import org.apache.storm.redis.common.mapper.RedisLookupMapper;
//import org.apache.storm.redis.common.mapper.RedisStoreMapper;
//import org.apache.storm.redis.trident.state.RedisClusterState;
//import org.apache.storm.redis.trident.state.RedisState;
//import org.apache.storm.trident.Stream;
//import org.apache.storm.trident.TridentState;
//import org.apache.storm.trident.TridentTopology;
//
//import java.net.InetSocketAddress;
//import java.util.HashSet;
//import java.util.Set;
//
//public class trident {
//        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
//                .setHost(redisHost).setPort(redisPort)
//                .build();
//        RedisStoreMapper storeMapper = new WordCountStoreMapper();
//        RedisLookupMapper lookupMapper = new WordCountLookupMapper();
//        RedisState.Factory factory = new RedisState.Factory(poolConfig);
//
//        TridentTopology topology = new TridentTopology();
//        Stream stream = topology.newStream("spout1", spout);
//
//        stream.partitionPersist(factory,fields,
//                new RedisStateUpdater(storeMapper).withExpire(86400000),
//                new Fields());
//
//        TridentState state = topology.newStaticState(factory);
//        stream = stream.stateQuery(state, new Fields("word"),
//                new RedisStateQuerier(lookupMapper),
//                new Fields("columnName","columnValue"));
//
//
//
//        Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
//        for (String hostPort : redisHostPort.split(",")) {
//            String[] host_port = hostPort.split(":");
//            nodes.add(new InetSocketAddress(host_port[0], Integer.valueOf(host_port[1])));
//        }
//        JedisClusterConfig clusterConfig = new JedisClusterConfig.Builder().setNodes(nodes)
//                .build();
//        RedisStoreMapper storeMapper = new WordCountStoreMapper();
//        RedisLookupMapper lookupMapper = new WordCountLookupMapper();
//        RedisClusterState.Factory factory = new RedisClusterState.Factory(clusterConfig);
//
//        TridentTopology topology = new TridentTopology();
//        Stream stream = topology.newStream("spout1", spout);
//
//        stream.partitionPersist(factory,fields,
//                new RedisClusterStateUpdater(storeMapper).withExpire(86400000),
//                new Fields());
//
//        TridentState state = topology.newStaticState(factory);
//        stream = stream.stateQuery(state, new Fields("word"),
//                new RedisClusterStateQuerier(lookupMapper),
//                new Fields("columnName","columnValue"));
//}
