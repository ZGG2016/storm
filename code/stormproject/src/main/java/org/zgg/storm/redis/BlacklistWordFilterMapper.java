package org.zgg.storm.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

public class BlacklistWordFilterMapper implements RedisFilterMapper {

    private RedisDataTypeDescription description;
    private final String setKey = "blacklist";

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.SET, setKey);
    }

    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    public String getValueFromTuple(ITuple tuple) {
        return null;
    }

}
