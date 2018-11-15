package test01.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author Kang
 */
public class NewConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        //定义的kafka服务的地址，指定的是kafka本地
        props.put("bootstrap.servers", "hadoop102:9092");
        //kafka消费者组----以组的形式读的topic的分组的数据
        props.put("group.id", "test1");
        //设置kafka-offset的reset
        props.put("auto.offset.reset", "earliest");

        //offset是否自动提交
        props.put("enable.auto.commit", "true");
        //offset多久提交一次
        props.put("auto.commit.interval.ms", "1000");
        //offset的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //获取消费者对象 --- 此处需要定义的泛型和 返回的record的数据一致
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //设置消费者订阅的topic主题，可以同时定义多个---- idea会自动的校验代码的合适
        consumer.subscribe(Collections.singletonList("first"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: " + record.topic() + "offset: " + record.offset() + ", partition: " + record.partition()+ " value: "+record.value());
            }
        }

    }
}
