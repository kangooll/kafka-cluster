package testInterceptor;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class TestConsumer {
    public static void main(String[] args)  throws  Exception{

        //配置consumer的配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "test1321");

        //设置 每次偏移量重置
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props);

        //订阅topic
        consumer.subscribe(Collections.singletonList("canal_kafka01"));
//        consumer2.subscribe(Collections.singletonList("first"));


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if(records.count()>0){
                for (ConsumerRecord<String, String> record : records) {

                    String str= new String(record.value().getBytes("gbk"),"utf-8");
                    System.out.println(record.partition()+"----"+record.offset()+"----"+str);
                }
            }
        }

//       ConsumerRecords<String, String> records2 = consumer2.poll(100);
//        if(records2.count()>0){
//            for (ConsumerRecord<String, String> record : records2) {
//                System.out.println(record.partition()+"----"+record.offset()+"----"+record.value());
//            }
//        }


    }
}
