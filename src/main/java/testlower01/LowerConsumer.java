package testlower01;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LowerConsumer {

    public static void main(String[] args) {
        List<String> brokers = Arrays.asList("hadoop102", "hadoop103","hadoop104");

        int port = 9092;

        String topic = "canal_kafka01";
        int partition = 1;
        long offset = 0;

        /*
        获取指定集群， 指定topic, 指定分区，指定offset的数据
         */
        getMessageByCondition(brokers,  port,  topic,  partition,  offset);
    }

    /**
     * 获取topic-partition-learder--供读取数据 --- 选择kafka--javaapi
     */
    public static BrokerEndPoint getLeader(List<String> brokers, int port, String topic, int partition){

        for (String broker : brokers) {
            SimpleConsumer simpleConsumer = new SimpleConsumer(broker, port, 1000, 1000, "test-first");//最后一个是组id
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));

            //返回分区的元数据信息
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadatas) {
                List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata();
                for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                    if(partitionMetadata.partitionId() == partition){
                        return partitionMetadata.leader();
                    }
                }
            }
        }
        return null;
    }

    /**
     * 根据leader和offset及topic等信息 读取分区数据
     */
    public  static void getMessageByCondition(List<String> brokers, int port, String topic, int partition, long offset){
        String host = getLeader(brokers, port, topic, partition).host();
        //最后一个是组id, --- 不一定和上面的一致
        SimpleConsumer simpleConsumer = new SimpleConsumer(host, port, 1000, 1000, "test-first");

        //获取数据的请求参数
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000).build();

        //消费者获取数据
        FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);

        //
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] buf = new byte[payload.limit()];
            //反序列化
            payload.get(buf);
            System.out.println("offset为："+messageAndOffset.offset()+"输出value: "+new String(buf));
        }
    }
}
