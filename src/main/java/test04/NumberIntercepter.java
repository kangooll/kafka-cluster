package test04;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Date:2018/10/18
 *
 * @author Kangol
 */
public class NumberIntercepter implements ProducerInterceptor<String,String> {

    private int sucessNum = 0;
    private int errorNum = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception !=null){
            errorNum ++;
        }else {
            sucessNum ++;
        }
    }

    @Override
    public void close() {
        System.out.println("sucess数量为："+sucessNum+"  失败的数量为："+errorNum);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
