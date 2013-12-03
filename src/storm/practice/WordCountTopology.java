package practice;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import practice.bolt.PrinterBolt;
import practice.bolt.RedisBolt;
import practice.bolt.SplitBolt;
import practice.spout.AmqpSentenceSpout;
import practice.spout.RandomSentenceSpout;

public class WordCountTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("random", new RandomSentenceSpout());
        builder.setSpout("amqp", new AmqpSentenceSpout());
        builder.setBolt("split", new SplitBolt()).shuffleGrouping("random").shuffleGrouping("amqp");
        builder.setBolt("printer", new PrinterBolt(), 8).shuffleGrouping("split");
        builder.setBolt("redis", new RedisBolt(), 8).shuffleGrouping("split");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("words", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
