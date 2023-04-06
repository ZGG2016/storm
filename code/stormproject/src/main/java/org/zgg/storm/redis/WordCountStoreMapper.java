package org.zgg.storm.redis;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

public class WordCountStoreMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public WordCountStoreMapper() {
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField("count");
    }
}
