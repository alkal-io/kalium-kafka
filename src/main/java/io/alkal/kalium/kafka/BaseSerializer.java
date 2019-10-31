package io.alkal.kalium.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public abstract class BaseSerializer implements Serializer<Object> {

    /**
     * Default constructor needed by Kafka
     */
    public BaseSerializer() {
    }


    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null)
            return null;

        try {
            return serializeImpl(data);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }

    abstract byte[] serializeImpl(Object data) throws Exception;

}

