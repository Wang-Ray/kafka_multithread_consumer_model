package wang.ray.sample.kafka.consumerthread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author ray
 */
@Slf4j
public class ConsumerThreadHandler implements Runnable {
    private ConsumerRecord consumerRecord;

    public ConsumerThreadHandler(ConsumerRecord consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    @Override
    public void run() {
        log.info("Consumer Message:" + consumerRecord.value() + ",Partition:" + consumerRecord.partition() + ",Offset:" + consumerRecord.offset());
    }
}
