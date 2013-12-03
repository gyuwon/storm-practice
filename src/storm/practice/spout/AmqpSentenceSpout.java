package practice.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Map;

public class AmqpSentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private QueueingConsumer _consumer;

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri("amqp://gyuwon:111111@localhost:5672/storm");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            _consumer = new QueueingConsumer(channel);
            channel.basicConsume(/* queue */"practice", /* autoAck */true, _consumer);
        }
        catch (Exception e) {
            System.out.println(e);
            _consumer = null;
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            _consumer.getChannel().getConnection().close();
        }
        catch (IOException e) {
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void nextTuple() {
        try {
            QueueingConsumer.Delivery delivery = _consumer.nextDelivery();
            if (delivery != null) {
                String sentence = new String(delivery.getBody());
                _collector.emit(new Values(sentence));
                return;
            }
        }
        catch (InterruptedException e) {
        }
        Utils.sleep(100);
    }
}
