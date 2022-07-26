package hu.balasz.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class StreamWordCountApp {
    private static final Logger logger = LoggerFactory.getLogger(StreamWordCountApp.class.getSimpleName());
    private static final String INPUT_TOPIC = "word-count-input";
    private static final String OUTPUT_TOPIC = "word-count-output";

    public static void main(String[] args) {
        var streams = new KafkaStreams(getTopology(), getProperties());

        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology getTopology() {
        var builder = new StreamsBuilder();
        KStream<String, String> wordCountInputStream = builder.stream(INPUT_TOPIC);

        KTable<String, Long> wordCounts = wordCountInputStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(it -> Arrays.asList(it.split("\\W+")))
                .selectKey((key, it) -> it)
                .groupByKey()
                .count(Materialized.as("Counts"));

        wordCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.149.48:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
