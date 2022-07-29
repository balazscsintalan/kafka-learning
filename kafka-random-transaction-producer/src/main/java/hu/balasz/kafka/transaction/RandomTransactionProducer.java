package hu.balasz.kafka.transaction;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class RandomTransactionProducer {
    private static final Logger logger = LoggerFactory.getLogger(RandomTransactionProducer.class.getSimpleName());

    public static final String BOOTSTRAP_SERVERS = "172.27.153.126:9092";

    // kafka-topics.sh --create --topic bank-transactions --bootstrap-server 172.27.153.126:9092 --partitions 2
    public static final String TRANSACTION_TOPIC = "bank-transactions";


    public static void main(String[] args) {
        try (Producer<String, String> producer = new KafkaProducer<>(getProducerProperties())) {
            int i = 0;
            while (true) {
                logger.info("Producing batch: {}", i);
                try {
                    producer.send(newRandomTransaction("john"));
                    Thread.sleep(100);
                    producer.send(newRandomTransaction("balasz"));
                    Thread.sleep(100);
                    producer.send(newRandomTransaction("alice"));
                    Thread.sleep(100);
                    i += 1;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private static ProducerRecord<String, String> newRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        var amount = ThreadLocalRandom.current().nextInt(0, 100);
        var now = Instant.now();
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>(TRANSACTION_TOPIC, name, transaction.toString());
    }

    private static Properties getProducerProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }
}
