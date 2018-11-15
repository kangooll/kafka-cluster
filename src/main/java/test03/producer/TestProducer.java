package test03.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class TestProducer {
    public static void main(String[] args) {

        //配置producer信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        //当producer将数据发送给
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        //指定那个partition的分析
        //props.put("partitioner.class", "test03.producer.TestPartition");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i =21; i< 30; i++) {
            producer.send(new ProducerRecord("three", "three"+i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(metadata != null){
                        System.out.println("partition: "+metadata.partition()+ "  offset: "+ metadata.offset());
                    }
                }
            });
        }
        //不设置close 发生异常
        producer.close();

    }
}
