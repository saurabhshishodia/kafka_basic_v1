package kafka.main;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("jsonTopic"));
        System.out.println("started consuming topic(s) "+Arrays.asList("jsonTopic"));
        int counter = 0;
        while (counter <= 1000) {
            ConsumerRecords<String, String> recs = consumer.poll(10);
            if (recs.count() == 0) {
            	//System.out.println("No value found");
            } else {
                for (ConsumerRecord<String, String> rec : recs) {
                    System.out.println("Recieved value "+rec.value());
                }
            }
            counter++;
        }
        
        System.out.println("Consumed all messages topic(s) "+Arrays.asList("jsonTopic"));
    }
}

