package wang.ray.sample.kafka.consumergroup;

import wang.ray.sample.kafka.ProducerThread;

/**
 * 多消费者消费
 *
 * @author ray
 */
public class ConsumerGroupMain {

    public static void main(String[] args) {
        String brokers = "localhost:9092,localhost:9093,localhost:9094";
        String groupId = "group01";
        String topic = "HelloWorld1";
        // 3个消费者
        int consumerNumber = 3;

        Thread producerThread = new Thread(new ProducerThread(brokers, topic));
        producerThread.start();

        ConsumerGroup consumerGroup = new ConsumerGroup(brokers, groupId, topic, consumerNumber);
        consumerGroup.start();
    }
}
