package wang.ray.sample.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 生产者
 *
 * @author ray
 */
@Slf4j
public class ProducerThread implements Runnable {
    private final Producer<String, String> kafkaProducer;
    private final String topic;

    public ProducerThread(String brokers, String topic) {
        Properties properties = buildKafkaProperty(brokers);
        this.topic = topic;
        this.kafkaProducer = new KafkaProducer<>(properties);

    }

    private static Properties buildKafkaProperty(String brokers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义分区
        properties.put("partitioner.class", "com.randy.CustomPartitioner");
        return properties;
    }

    @Override
    public void run() {
        System.out.println("start sending message to kafka");
        int i = 0;
        while (true) {
            String sendMsg = "Producer message number:" + ++i;
            kafkaProducer.send(new ProducerRecord<>(topic, sendMsg, sendMsg), new Callback() {

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    log.info("Producer Message: Partition:" + recordMetadata.partition() + ",Offset:" + recordMetadata.offset());
                }
            });
            // thread sleep 3 seconds every time
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            log.debug("end sending message to kafka");
        }
    }
}
