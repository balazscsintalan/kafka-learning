package hu.balasz.kafka.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<JsonNode> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        // empty
    }


    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        try {
            return objectMapper.readTree(bytes);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }


    @Override
    public void close() {
        // empty
    }
}
