package io.alkal.kalium.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JsonDeSerializer implements Deserializer<Object> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, Class<?>> topicToClassMap;

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeSerializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {

        topicToClassMap = (Map<String, Class<?>>) props.get("topicToClassMap");
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0 || !topicToClassMap.containsKey(topic))
            return null;

        Object data;
        try {
            data = objectMapper.readValue(bytes, topicToClassMap.get(topic));
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}