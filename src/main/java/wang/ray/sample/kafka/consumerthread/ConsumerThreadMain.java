package wang.ray.sample.kafka.consumerthread;

import wang.ray.sample.kafka.ProducerThread;

/**
 * 单消费者多线程消费
 *
 * @author ray
 */
public class ConsumerThreadMain {

    public static void main(String[] args) {
        String brokers = "localhost:9092,localhost:9093,localhost:9094";
        String groupId = "group01";
        String topic = "HelloWorld1";
        // 3个线程
        int consumerTheadNumber = 3;


        Thread producerThread = new Thread(new ProducerThread(brokers, topic));
        producerThread.start();

        ConsumerThread consumerThread = new ConsumerThread(brokers, groupId, topic);
        consumerThread.start(consumerTheadNumber);


    }
}
