package test03.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class TestConsumer {
    public static void main(String[] args) {

        //配置consumer的配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "test");

        //设置每次都从头开始读
        props.put("auto.offset.reset", "earliest");

        //设置自动提交
        props.put("enable.auto.commit", "true");
        //消息的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props);
        //订阅某个主题
        consumer1.subscribe(Collections.singletonList("first"));
        //接收消息
        boolean flag =true;

        while (flag){
            ConsumerRecords<String, String> records1 = consumer1.poll(100);
            for (ConsumerRecord<String, String> record : records1) {
                System.out.println("consumer1 partition: "+record.partition()+", offset: "+record.offset()+", value: "+record.value());
            }
        }
    }
}
