package hu.balasz.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColorStreamApp {
    private static final Logger logger = LoggerFactory.getLogger(FavouriteColorStreamApp.class.getSimpleName());
    private static final String INPUT_TOPIC = "favourite-color-input";
    private static final String OUTPUT_TOPIC = "favourite-color-output";
    private static final String COMPACTED = "favourite-color-compacted";
    private static final List<String> allowedColors = Arrays.asList("green", "red", "blue");

    public static void main(String[] args) {
        var streams = new KafkaStreams(getTopology(), getProperties());

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology getTopology() {
        var builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        var keyedStream = inputStream
                .map((key, value) -> {
                    var split = value.split(",");
                    return new KeyValue<>(split[0], split[1]);
                })
                .filter((key, value) -> allowedColors.contains(value));


        keyedStream.to(COMPACTED);

        var count = builder.table(COMPACTED, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((key, value) -> KeyValue.pair(value, key))
                .count();

        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.158.25:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
