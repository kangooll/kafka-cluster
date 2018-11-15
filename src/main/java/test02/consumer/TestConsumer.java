package test02.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class TestConsumer {

    public static void main(String[] args) {
        //kafka消费者案例
        Properties props = new Properties();
        //kafka节点的端口
        props.put("bootstrap.servers", "hadoop102:9092");
        //kafka是以组为单位 读数据的
        props.put("group.id", "test111");
        //自动提交关闭---需要每次手动提交
        props.put("enable.auto.commit", "false");

        //手动设置一次 偏移量，不设置将按照默认的偏移量开始读
        props.put("auto.offset.reset","earliest");

        //kafka consumer端的 反序列化方法
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props);
        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props);
        KafkaConsumer<String, String> consumer3 = new KafkaConsumer<>(props);

        //consumer订阅topic的主题为 second
        consumer1.subscribe(Collections.singletonList("second"));
        consumer2.subscribe(Collections.singletonList("second"));
        consumer3.subscribe(Collections.singletonList("second"));

        boolean flag = true;
        while (flag){
            ConsumerRecords<String, String> records1 = consumer1.poll(100);
            for (ConsumerRecord<String, String> record : records1) {
                System.out.println("consumer1--partition为"+record.partition()+" offset: "+record.offset()+" value: "+record.value());
            }

            ConsumerRecords<String, String> records2 = consumer1.poll(100);
            for (ConsumerRecord<String, String> record : records2) {
                System.out.println("consumer2--partition为"+record.partition()+" offset: "+record.offset()+" value: "+record.value());
            }

            ConsumerRecords<String, String> records3 = consumer1.poll(100);
            for (ConsumerRecord<String, String> record : records3) {
                System.out.println("consumer3--partition为"+record.partition()+" offset: "+record.offset()+" value: "+record.value());
            }
            System.out.println("-----------------------分割线------------------------");

            if(records1.count()==0 && records2.count()==0  || records3.count()==0 ){
                flag = false;
            }
        }

    }
}
