package testInterceptor;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.security.spec.ECField;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestProducer {
    public static void main(String[] args) {
        //配置producer项目
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        List<String> interceptors = new ArrayList<>();
        //interceptor是按照顺序执行的
        interceptors.add("testInterceptor.TimeTempInterceptor");
        interceptors.add("testInterceptor.NumberInterceptor");
        props.put("interceptor.classes", interceptors);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i =10; i <20; i++){
            producer.send(new ProducerRecord<String, String>("first", String.valueOf(i), "first-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //增加判断
                    if(exception != null){
                        System.out.println("分区为: "+metadata.partition()+" 偏移量为: "+metadata.offset());
                    }
                }
            });
        }

        //需要关闭
        producer.close();
    }
}
