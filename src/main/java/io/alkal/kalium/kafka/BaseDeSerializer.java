package io.alkal.kalium.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author Ziv Salzman
 * Created on 16-Oct-2019
 */
public abstract class BaseDeSerializer implements Deserializer<Object> {


    protected Map<String, Class<?>> topicToClassMap;


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
            data = deserializeImpl(bytes, topicToClassMap.get(topic));
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }

    protected final Map<String, Class<?>> getTopicToClassMap(){
        return topicToClassMap;
    }

    abstract Object deserializeImpl(byte[] bytes, Class<?> clazz) throws Exception;

}
