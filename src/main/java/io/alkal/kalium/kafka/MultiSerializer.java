package io.alkal.kalium.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MultiSerializer implements Serializer<Object> {

    private JsonSerializer jsonSerializer;
    private ProtobufSerializer protobufSerializer;

    /**
     * Default constructor needed by Kafka
     */
    public MultiSerializer() {
        this.jsonSerializer = new JsonSerializer();
        this.protobufSerializer = new ProtobufSerializer();
    }

    //for testing
    public MultiSerializer(JsonSerializer jsonSerializer, ProtobufSerializer protobufSerializer) {
        this.jsonSerializer = jsonSerializer;
        this.protobufSerializer = protobufSerializer;
    }


    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        jsonSerializer.configure(props, isKey);
        protobufSerializer.configure(props, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (ProtoUtils.isProtoClass(data.getClass())) {
            return protobufSerializer.serialize(topic, data);
        }
        return jsonSerializer.serialize(topic, data);
    }

    @Override
    public void close() {
        jsonSerializer.close();
        protobufSerializer.close();
    }


}

