package io.alkal.kalium.kafka.tests;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import io.alkal.kalium.Kalium;
import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.kafka.KaliumKafkaQueueAdapter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KaliumKafkaBasicTest {

    public static final String KAFKA_ENDPOINT = "localhost:9092";

    @Test
    public void testItShouldCallReactorMethod_whenAMatchingEventIsPosted() throws InterruptedException {

        System.out.println("Start Kalium-Kafka Basic End-2-End Test");
        KaliumQueueAdapter queueAdapter1 = new KaliumKafkaQueueAdapter(KAFKA_ENDPOINT);

        MyReactor myReactor = Mockito.spy(new MyReactor());
        MyReactor2 myReactor2 = Mockito.spy(new MyReactor2());
        Kalium kalium1 = Kalium.Builder()
                .setQueueAdapter(queueAdapter1)
                .addReactor(myReactor)
                .addReactor(myReactor2)
                .build();

        kalium1.start();

        KaliumQueueAdapter queueAdapter2 = new KaliumKafkaQueueAdapter(KAFKA_ENDPOINT);
        Kalium kalium2 = Kalium.Builder()
                .setQueueAdapter(queueAdapter2)
                .build();
        kalium2.start();

        Payment payment = new Payment();
        payment.setId(UUID.randomUUID().toString());

        kalium2.post(payment);


        Thread.sleep(6000);

        synchronized (myReactor) {
            ArgumentCaptor<Payment> argumentCaptor = ArgumentCaptor.forClass(Payment.class);
            Mockito.verify(myReactor).doSomething(argumentCaptor.capture());
            Payment capturedArgument = argumentCaptor.<Payment>getValue();
            Assert.assertEquals(capturedArgument.getId(), payment.getId());
        }

        synchronized (myReactor2) {
            ArgumentCaptor<Payment> argumentCaptor = ArgumentCaptor.forClass(Payment.class);
            Mockito.verify(myReactor2).doSomething(argumentCaptor.capture());
            Payment capturedArgument = argumentCaptor.<Payment>getValue();
            Assert.assertEquals(capturedArgument.getId(), payment.getId());
        }


    }


}
