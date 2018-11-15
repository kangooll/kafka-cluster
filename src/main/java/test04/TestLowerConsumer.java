package test04;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;


import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Date:2018/10/18
 *
 * @author Kangol
 */
public class TestLowerConsumer {
    public static void main(String[] args) {
        List<String> brokers = Arrays.asList("hadoop102", "hadoop103", "hadoop104");
        int port = 9092;
        String topic = "first";
        int partition = 1;
        long offset = 10L;

        //低级api读取消息
        getMessageByOffSet(brokers, port, topic, partition, offset);

    }


    /**
     * 理解消费者 读数据的流程， 根据zk找到topic，然后根据topic的多个partition，找到每个parititon的leader, consumer从leader里面读数据
     * 如果是多个partition 则consumer以轮训的方式读数据
     *
     * @param brokers
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public static BrokerEndPoint getPartitionLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {

            SimpleConsumer simpleConsumer = new SimpleConsumer(broker, port, 1000, 1000, "test-first");
            //目的获取kafka的topic元数据
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            TopicMetadataResponse send = simpleConsumer.send(topicMetadataRequest);

            //获取每个topic的元数据信息
            for (TopicMetadata topicsMetadatum : send.topicsMetadata()) {
                //获取每个partition分区的元数据信息
                List<PartitionMetadata> partitionMetadata = topicsMetadatum.partitionsMetadata();
                for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                    if (partitionMetadatum.partitionId() == partition) {
                        return partitionMetadatum.leader();
                    }
                }
            }
        }
        return null;
    }


    //消费数据，从某个offset开始
    public static void getMessageByOffSet(List<String> brokers, int port, String topic, int partition, long offset) {
        BrokerEndPoint partitionLeader = getPartitionLeader(brokers, port, topic, partition);
        String host = partitionLeader.host();
        //根据某个分区leader在的位置，读数据

        SimpleConsumer simpleConsumer = new SimpleConsumer(host, port, 1000, 1000, "test-first");

        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000).build();
        FetchResponse fetch = simpleConsumer.fetch(fetchRequest);

        ByteBufferMessageSet messageAndOffsets = fetch.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] buf = new byte[payload.limit()];
            payload.get(buf);
            System.out.println("offset为：" + messageAndOffset.offset() + " 消息的内容为：" + new String(buf));
        }

        System.out.println("-----------------------------\r\n");

        Long lastOffset = getLastOffset(host, port, topic, partition);
        System.out.println("最新的offset为：" + lastOffset);
    }

    public static Long getLastOffset(String broker, int port, String topic, int partition) {
        SimpleConsumer simpleConsumer = new SimpleConsumer(broker, port, 1000, 1000, "test-first");

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

        //必须设置为 最近的时间 LatestTime 默认值是-1L
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo( kafka.api.OffsetRequest.LatestTime(), 1));

        //版本默认是0
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), "test-02-kangol");

        //获取
        OffsetResponse offsetsBefore = simpleConsumer.getOffsetsBefore(request);


        //如果没读到就返回0
        if (offsetsBefore.hasError()) {
            return 0L;
        }

        long[] offsets = offsetsBefore.offsets(topic, partition);
        if(offsets.length==0){
            return 0L;
        }else{
            return offsets[0];
        }
    }

}
