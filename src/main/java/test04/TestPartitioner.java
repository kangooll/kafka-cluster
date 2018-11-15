package test04;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Date:2018/10/17
 *
 * @author Kangol
 */
public class TestPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int num = partitions.size();
        int partNum = 0;
        try {
            partNum = Integer.parseInt((String) key);
        } catch (Exception e) {
            partNum = key.hashCode();
        }
        return Math.abs(partNum % num);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
