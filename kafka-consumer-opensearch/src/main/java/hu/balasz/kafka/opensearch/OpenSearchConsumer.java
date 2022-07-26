package hu.balasz.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
    private static final String WIKIMEDIA_INDEX = "wikimedia";
    private static final String TOPIC = "wikimedia.recentchange";
    private static final String BOOSTRAP_SERVERS = "172.27.158.251:9092";
    private static final String GROUP_ID = "consumer-opensearch-demo";

    public static void main(String[] args) throws IOException {
        try (var openSearchClient = createOpenSearchClient();
             var kafkaConsumer = createKafkaConsumer()) {
            createIndexIfNotExists(openSearchClient);
            kafkaConsumer.subscribe(Collections.singleton(TOPIC));


            while (true) {
                var records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received {} record(s)", recordCount);

                var bulkRequest = new BulkRequest();

                for (var record : records) {
                    // make idempotent
                    // strategy 1
                    // define an ID using Kafka record coordinates
                    // var id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Strategy 2 - extract id from JSON value
                    var id = extractId(record.value());

                    var indexRequest = new IndexRequest(WIKIMEDIA_INDEX)
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    bulkRequest.add(indexRequest);
                }

                if (bulkRequest.numberOfActions() > 0) {
                    var response = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} records", response.getItems().length);
                    // commit offset after batch is consumed
                    kafkaConsumer.commitSync();
                    log.info("Offets have been committed");
                }
            }
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static void createIndexIfNotExists(RestHighLevelClient client) throws IOException {
        var indexExists = client.indices().exists(new GetIndexRequest(WIKIMEDIA_INDEX), RequestOptions.DEFAULT);
        if (indexExists) {
            log.info("WikiMedia index already exists");
        } else {
            client.indices().create(new CreateIndexRequest(WIKIMEDIA_INDEX), RequestOptions.DEFAULT);
            log.info("WikiMedia index has been created");
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // disable auto commit of offset, but then has to do it manually
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }


    private static RestHighLevelClient createOpenSearchClient() {
        var connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        var connUri = URI.create(connString);
        // extract login information if it exists
        var userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            var auth = userInfo.split(":");

            var cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }
}
