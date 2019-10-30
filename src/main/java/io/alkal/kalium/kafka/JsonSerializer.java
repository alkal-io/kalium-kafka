package io.alkal.kalium.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer extends BaseSerializer {

    private final ObjectMapper objectMapper;

    /**
     * Default constructor needed by Kafka
     */
    public JsonSerializer() {
        objectMapper = new ObjectMapper();
    }

    //for test purposes
    public JsonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public byte[] serializeImpl(Object data) throws Exception {
        return objectMapper.writeValueAsBytes(data);

    }


}

