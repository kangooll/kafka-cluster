package teststreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Kang
 */
public class TestStream {

    public static void main(String[] args) {
        //props参数
        Map<String, String> props = new HashMap<>();
        props.put("application.id", "logFilter-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        StreamsConfig streamsConfig = new StreamsConfig(props);


        //设置kafka流的拓扑结构 --类似flume的source--channel---sink
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source", "first")
                .addProcessor("process", LogProcessor::new, "source")
                .addSink("sink", "second", "process");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);

        //处理业务
        kafkaStreams.start();
    }

}
