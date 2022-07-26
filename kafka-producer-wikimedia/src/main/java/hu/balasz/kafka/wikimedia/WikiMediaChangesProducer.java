package hu.balasz.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaChangesProducer {

    public static final String BOOT_STRAP_SERVERS = "172.27.158.251:9092";
    public static final String TOPIC = "wikimedia.recentchange";
    public static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        var producer = new KafkaProducer<String, String>(getProducerProperties());
        var eventHandler = new WikiMediaChangeHandler(producer, TOPIC);
        var eventSource = new EventSource.Builder(eventHandler, URI.create(WIKIMEDIA_URL)).build();

        // start producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block until then
        TimeUnit.MINUTES.sleep(10);

    }

    private static Properties getProducerProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return properties;
    }
}
