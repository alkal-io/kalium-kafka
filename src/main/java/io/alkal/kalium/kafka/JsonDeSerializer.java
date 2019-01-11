package io.alkal.kalium.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeSerializer implements Deserializer<Object> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<?> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeSerializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<?>) props.get("pojo.class");
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        Object data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}