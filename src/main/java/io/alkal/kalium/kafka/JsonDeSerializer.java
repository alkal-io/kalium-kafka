package io.alkal.kalium.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;


public class JsonDeSerializer extends BaseDeSerializer {
    private ObjectMapper objectMapper;

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

    @Override
    Object deserializeImpl(byte[] bytes, Class<?> clazz) throws Exception {
        return objectMapper.readValue(bytes, clazz);
    }

}