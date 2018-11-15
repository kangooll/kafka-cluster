package test04;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import teststreams.LogProcessor;

import java.util.Properties;

/**
 * Date:2018/10/18
 *
 * @author Kangol
 */
public class TestApplication01 {
    public static void main(String[] args) {
        // 定义输入的topic
        String from = "first";
        // 定义输出的topic
        String to = "second";
        // 设置参数
        Properties settings = new Properties();
        //先不配置下面的两个配置，然后根据提示，输入参数
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        StreamsConfig config = new StreamsConfig(settings);
        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", new ProcessorSupplier<byte[], byte[]>() {
                    @Override
                    public Processor<byte[], byte[]> get() {
                        // 具体分析处理
                        return new LogProcessor01();
                    }
                }, "SOURCE")
                .addSink("SINK", to, "PROCESS");

        //创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

    }
}
