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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class KaliumKafkaQueueAdapter implements KaliumQueueAdapter {


    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private Properties kafkaProps;

    private Collection<ConsumerReactor<String, ?>> kafkaConsumers;

    private QueueListener queueListener;

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
        if (kafkaConsumers == null || kafkaConsumers.size() == 0) return;
        ExecutorService executorService = Executors.newFixedThreadPool(kafkaConsumers.size());
        kafkaConsumers.stream().map(consumer ->
                executorService.submit(new Callable<Object>() {

                    @Override
                    public Object call() throws Exception {
                        while (true) {
                            ConsumerRecords<String, ?> records = consumer.poll(Duration.ofSeconds(1L));
                            for (ConsumerRecord<String, ?> record : records)
                                queueListener.onObjectReceived(consumer.getReactor(), record.value());
                        }
                    }
                })
        ).collect(Collectors.toList());
    }


    @Override
    public void post(Object o) {
        Producer<String, Object> producer = new KafkaProducer<>(kafkaProps);
        producer.send(new ProducerRecord<String, Object>(o.getClass().getSimpleName(), o));
        producer.close();
    }

    @Override
    public void setQueueListener(QueueListener queueListener) {
        this.queueListener = queueListener;
        kafkaConsumers = new ArrayList<>();
        this.queueListener.getReactorToObjectTypeMap().entrySet().stream().forEach(entry -> {
            Collection<ConsumerReactor<String, ?>> consumers = entry.getValue().stream().map(objectType ->
                    bindReactor(entry.getKey(), objectType)).collect(Collectors.toList());
            kafkaConsumers.addAll(consumers);
        });
    }

    private ConsumerReactor<String, ?> bindReactor(Class<?> reactorClass, Class<?> objectType) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, kafkaProps.getProperty(BOOTSTRAP_SERVERS));
        props.put("group.id", reactorClass.getName());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.alkal.kalium.kafka.JsonSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.alkal.kalium.kafka.JsonDeSerializer");
        props.put("pojo.class", objectType);

        ConsumerReactor<String, Object> consumer = new ConsumerReactor<>(props);

        consumer.subscribe(Collections.singletonList(objectType.getSimpleName()));
        consumer.setReactor(reactorClass);
        return consumer;

    }


}
