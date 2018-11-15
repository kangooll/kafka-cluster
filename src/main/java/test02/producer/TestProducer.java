package test02.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class TestProducer {
    public static void main(String[] args) {
        //声明的kafka的参数
        Properties props = new Properties();
        //kafka集群的某个节点
        props.put("bootstrap.servers", "hadoop102:9092");
        //kafka发送数据，直到各个分区的leader将数据传送到follower后，才显示发送成功
        props.put("acks", "all");
        //重试次数
        props.put("retries", 0);
        //producer每次发送16k的数据
        props.put("batch.size", 16384);
        //每次持续传输的时间为1ms ，如果没传递16k，也终止
        props.put("linger.ms", 1);
        //kafkaproducer的数据缓存区为32m
        props.put("buffer.memory", 33554432);

        //producer设置发送的分区
        //props.put("partitioner.class","test02.producer.CustomerPartitioner");

        //kafka传递数据 需要将key和value序列化 --- org.apache.kafka.common.serialization 这个包下有很多序列化的类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 100; i < 120; i++) {
            //执行没法送一次数据 都有返回值回调的方法
            producer.send(new ProducerRecord<String, String>("second", String.valueOf(i), "second" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(metadata != null){
                        System.out.println("topic："+metadata.topic()+" partition: "+metadata.partition()+" offset: "+metadata.offset());
                    }
                }
            });
        }

        //必须设置关闭 才行
        producer.close();

    }
}
