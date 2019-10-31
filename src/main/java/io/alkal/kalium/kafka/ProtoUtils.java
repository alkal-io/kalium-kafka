package io.alkal.kalium.kafka;

import java.lang.reflect.Method;

/**
 * @author Ziv Salzman
 * Created on 29-Oct-2019
 */
public class ProtoUtils {

    public static Method getParseFromMethod(Class<?> clazz) throws NoSuchMethodException {
        return clazz.getMethod("parseFrom", byte[].class);
    }

    public static boolean isProtoClass(Class<?> clazz) {
        try {
            getParseFromMethod(clazz);
        } catch (NoSuchMethodException nsme) {
            return false;
        }

        return true;
    }


}
