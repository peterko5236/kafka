import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerSync {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "peter-kafka001.foo.bar:9092,peter-kafka002.foo.bar:9092,peter-kafka003.foo.bar:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("peter-topic", "Apache Kafka is a distributed streaming platform - " + i);
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Received Message: %s\n", metadata.topic(), metadata.partition()
                , metadata.offset(), record.key(), record.value());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
