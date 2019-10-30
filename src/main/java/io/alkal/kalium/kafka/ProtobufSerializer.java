package io.alkal.kalium.kafka;

import java.lang.reflect.Method;

public class ProtobufSerializer extends BaseSerializer {

    @Override
    public byte[] serializeImpl(Object data) throws Exception {
        Method method = data.getClass().getMethod("toByteArray");
        return (byte[]) method.invoke(data);

    }

}

