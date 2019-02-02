package io.alkal.kalium.kafka;

import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.internals.QueueListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class KaliumKafkaQueueAdapter implements KaliumQueueAdapter {


    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private Properties kafkaProps;

    private Collection<ConsumerReactor<String, ?>> kafkaConsumers;

    private QueueListener queueListener;
    private ExecutorService postingExecutorService;

    public KaliumKafkaQueueAdapter(String kafkaEndpoint) {
        kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS, kafkaEndpoint);
        kafkaProps.put("acks", "all");
//        kafkaProps.put("delivery.timeout.ms", 30000);
        kafkaProps.put("batch.size", 16384);
//        kafkaProps.put("linger.ms", 30000);
        kafkaProps.put("buffer.memory", 33554432);

        StringSerializer ss;
        //TODO provide kalium implementation for these ones
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "io.alkal.kalium.kafka.JsonSerializer");
//        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Override
    public void start() {
        Collection<Class<?>> reactorClasses = this.queueListener.getReactorToObjectTypeMap().keySet();
        if (reactorClasses != null && reactorClasses.size() > 0) {
            ExecutorService executorService = Executors.newFixedThreadPool(reactorClasses.size());
            List<ConsumerLoop> consumers = queueListener.getReactorToObjectTypeMap().entrySet().stream().map(reactorEntry ->
                    new ConsumerLoop(reactorEntry.getKey(), reactorEntry.getValue(), this.queueListener)
            ).collect(Collectors.toList());
            consumers.forEach(consumer -> executorService.submit(consumer));

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    for (ConsumerLoop consumer : consumers) {
                        consumer.shutdown();
                    }
                    executorService.shutdown();
                    try {
                        executorService.awaitTermination(5000, TimeUnit.DAYS.MILLISECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        postingExecutorService = Executors.newFixedThreadPool(10);

    }


    @Override
    public void post(Object o) {
        postingExecutorService.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("Object is about to be sent");
                Producer<String, Object> producer = new KafkaProducer<>(kafkaProps);
                producer.send(new ProducerRecord<String, Object>(o.getClass().getSimpleName(), o));

                producer.close();
                System.out.println("Object sent");
            }
        });

    }

    @Override
    public void setQueueListener(QueueListener queueListener) {
        this.queueListener = queueListener;
    }

//    private ConsumerReactor<String, ?> bindReactor(Class<?> reactorClass, Class<?> objectType) {
//        Properties props = new Properties();
//        props.put(BOOTSTRAP_SERVERS, kafkaProps.getProperty(BOOTSTRAP_SERVERS));
//        props.put("group.id", reactorClass.getName());
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "30000");
//
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "io.alkal.kalium.kafka.JsonSerializer");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "io.alkal.kalium.kafka.JsonDeSerializer");
//        props.put("pojo.class", objectType);
//
//        ConsumerReactor<String, Object> consumer = new ConsumerReactor<>(props);
//
//        consumer.subscribe(Collections.singletonList(objectType.getSimpleName()));
//        consumer.setReactor(reactorClass);
//        return consumer;
//
//    }


}
