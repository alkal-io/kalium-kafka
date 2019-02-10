package io.alkal.kalium.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class JsonDeSerializer implements Deserializer<Object> {
    private ObjectMapper objectMapper;

    private Map<String, Class<?>> topicToClassMap;

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    //for test purposes
    public JsonDeSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        if (props == null || !props.containsKey(Constants.TOPIC_TO_CLASS_MAP)
                || props.get(Constants.TOPIC_TO_CLASS_MAP) == null) {
            throw new RuntimeException("Failed to configure de-serializer. Missing " + Constants.TOPIC_TO_CLASS_MAP + " value");
        }
        topicToClassMap = (Map<String, Class<?>>) props.get(Constants.TOPIC_TO_CLASS_MAP);
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if (topic == null || topic.isEmpty() || !topicToClassMap.containsKey(topic) || topicToClassMap.get(topic) == null) {
            throw new SerializationException("Kalium failed to desrialize message as topic is either null or no class is mapped to this topic!");
        }


            if (bytes == null || bytes.length == 0) {
                return null;
            }

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