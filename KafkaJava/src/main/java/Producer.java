import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String server = "127.0.0.1:9092";
        String topic = "user_registered";

        Producer producer = new Producer(server);
        producer.put(topic, "user1", "Misha");
        producer.put(topic, "user2", "Vasya");
        producer.put(topic, "user3", "Alexey");
        producer.put(topic, "user4", "Marina");
        producer.put(topic, "user5", "Pavel");
        producer.close();
    }

    // Variables

    private final KafkaProducer<String, String> producer;

    // Constructors

    Producer(String bootstrapServer) {
        Properties props = producerProps(bootstrapServer);
        producer = new KafkaProducer<>(props);

        log.info("Producer initialized");
    }

    // Public

    void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        log.info("Put value: " + value + ", for key: " + key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {

                log.info("Error while producing");
                return;
            }

            log.info("Received new meta. Topic: " + recordMetadata.topic()
                    + "; Partition: " + recordMetadata.partition()
                    + "; Offset: " + recordMetadata.offset()
                    + "; Timestamp: " + recordMetadata.timestamp());
        }).get();
    }

    void close() {
        log.info("Closing producer's connection");
        producer.close();
    }

    // Private

    private Properties producerProps(String bootstrapServer) {
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

        return props;
    }
}

