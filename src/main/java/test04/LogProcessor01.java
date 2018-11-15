package test04;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Date:2018/10/18
 *
 * @author Kangol
 */
public class LogProcessor01 implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        //需要转换下，直接value.totring()之类的不管用，需要按照string的
        String valueStr = new String(value);

        // 如果包含“>>>”则只保留该标记后面的内容
        if(valueStr.contains(">>>")){
            valueStr = valueStr.split(">>>")[1];
        }
        context.forward(key, valueStr.getBytes());
    }

    @Override
    public void punctuate(long timestamp) {
    }

    @Override
    public void close() {
    }
}
