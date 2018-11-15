package testInterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class NumberInterceptor implements ProducerInterceptor<String, String> {

    private int success=0;
    private int error = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            success ++;
        }else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("正确的记录条数为："+success);
        System.out.println("错误的记录条数为："+error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
