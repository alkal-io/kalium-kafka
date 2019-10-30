package io.alkal.kalium.kafka.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import io.alkal.kalium.kafka.Constants;
import io.alkal.kalium.kafka.JsonSerializer;
import io.alkal.kalium.kafka.MultiSerializer;
import io.alkal.kalium.kafka.ProtobufSerializer;
import io.alkal.kalium.kafka.tests.pb.Payment;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MultiSerializerTest {

    private MultiSerializer target;

    private JsonSerializer jsonSerializer;

    private ProtobufSerializer protobufSerializer;

    @Before
    public void setup() {
        protobufSerializer = spy(ProtobufSerializer.class);
        jsonSerializer = spy(JsonSerializer.class);
        target = new MultiSerializer(jsonSerializer, protobufSerializer);
    }

    @Test
    public void testConfigure_shouldCallConfigureInDelagateSerializers() {
        boolean isKey = true;
        Map<String, Object> props = createValidProps();
        target.configure(props, isKey);
        verify(jsonSerializer).configure(eq(props), eq(isKey));
        verify(protobufSerializer).configure(eq(props), eq(isKey));
    }

    @Test(expected = RuntimeException.class)
    public void testConfigure_shouldThrowAnException_whenDelegatSerializerThrowsException() throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        doThrow(new RuntimeException()).when(jsonSerializer).configure(null, false);
        target.configure(null, false);
    }


    @Test(expected = SerializationException.class)
    public void testSerialize_shouldThrowAnException_whenDelegatSerializerThrowsException() throws NoSuchMethodException {
        doThrow(new SerializationException()).when(jsonSerializer).serialize(any(), any());
        target.serialize("payment", new io.alkal.kalium.kafka.tests.Payment());
    }

    @Test
    public void testSerialize_shouldDelegateToProtoSerialize_whenTopicIsForProtoObject() {
        Map<String, Object> props = createValidProps();
        target.configure(props, false);
        byte[] data = new byte[3];
        Payment.PaymentPB paymentPB = Payment.PaymentPB.newBuilder().build();
        verify(jsonSerializer, never()).serialize(any(), any());
        byte[] bytes = target.serialize("paymentPb", paymentPB);
        assertArrayEquals(paymentPB.toByteArray(), bytes);

    }

    @Test
    public void testSerialize_shouldDelegateToJsonSerialize_whenTopicIsNotForProtoObject() throws Exception {
        Map<String, Object> props = createValidProps();
        target.configure(props, false);
        byte[] data = new byte[3];
        io.alkal.kalium.kafka.tests.Payment payment = new io.alkal.kalium.kafka.tests.Payment();
        verify(protobufSerializer, never()).serialize(anyString(), any(Object.class));
        byte[] bytes = target.serialize("payment", payment);
        assertArrayEquals(new ObjectMapper().writeValueAsBytes(payment), bytes);

    }

    @Test
    public void testClose_shouldCallCloseInDelagateSerializers() {

        target.close();
        verify(jsonSerializer).close();
        verify(protobufSerializer).close();
    }


    private static Map<String, Object> createValidProps() {
        Map<String, Object> props = new HashMap<>();
        Map<String, Class> topicToClassMap = new HashMap<>();
        topicToClassMap.put("paymentPb", Payment.PaymentPB.class);
        topicToClassMap.put("payment", io.alkal.kalium.kafka.tests.Payment.class);
        props.put(Constants.TOPIC_TO_CLASS_MAP, topicToClassMap);
        return props;
    }


}
