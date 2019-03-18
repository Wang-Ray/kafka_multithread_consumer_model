package wang.ray.sample.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
 * 自定义分区
 *
 * @author ray
 */
@Slf4j
public class CustomPartitioner implements Partitioner {

    private Random random;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitionCount = cluster.availablePartitionsForTopic(topic).size();
        log.debug("topic {} has {} partitions", topic, partitionCount);
        return random.nextInt(partitionCount);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        random = new Random();
    }
}
