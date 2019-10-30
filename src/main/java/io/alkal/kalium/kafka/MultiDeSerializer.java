package io.alkal.kalium.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public class MultiDeSerializer implements Deserializer<Object> {


    private JsonDeSerializer jsonDeSerializer;
    private ProtobufDeSerializer protobufDeSerializer;

    public MultiDeSerializer() {
        this.jsonDeSerializer = new JsonDeSerializer();
        this.protobufDeSerializer = new ProtobufDeSerializer();
    }

    //for testing
    public MultiDeSerializer(JsonDeSerializer jsonDeSerializer, ProtobufDeSerializer protobufDeSerializer) {
        this.jsonDeSerializer = jsonDeSerializer;
        this.protobufDeSerializer = protobufDeSerializer;
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {

        jsonDeSerializer.configure(props, isKey);
        protobufDeSerializer.configure(props, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if (isProtobuf(topic)) {
            return protobufDeSerializer.deserialize(topic, bytes);

        } else {
            return jsonDeSerializer.deserialize(topic, bytes);
        }
    }

    @Override
    public void close() {
        jsonDeSerializer.close();
        protobufDeSerializer.close();
    }

    //visible for testing
    public boolean isProtobuf(String topic) {
        return ProtoUtils.isProtoClass(protobufDeSerializer.getTopicToClassMap().get(topic));
    }




}
