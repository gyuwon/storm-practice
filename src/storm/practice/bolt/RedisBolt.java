package practice.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisBolt extends BaseBasicBolt {
    private Jedis _redis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        _redis = new Jedis("localhost");
    }

    @Override
    public void cleanup() {
        super.cleanup();
        _redis.disconnect();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getStringByField("word");
        _redis.incr(word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
