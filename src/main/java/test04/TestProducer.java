package test04;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class TestProducer {
    public static void main(String[] args) {

        //书写样板使用的  KafkaProducer 类的例子， 查询配置参数的含义 使用的ProducerConfig类

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("partitioner.class", "test04.TestPartitioner");

        List<String> interceptors = new ArrayList<>();
        interceptors.add("test04.TimeIntercepter");
        interceptors.add("test04.NumberIntercepter");
        props.put("interceptor.classes",interceptors);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("first", "kang_" + i, "00" + Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println(metadata.topic()+" -- "+metadata.offset());
                    }
                }
            });
        }
        //提交资源
        producer.close();
    }
}
