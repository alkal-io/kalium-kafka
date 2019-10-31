package io.alkal.kalium.kafka;

import io.alkal.kalium.internals.QueueListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Ziv Salzman
 * Created on 01-Feb-2019
 */
public class ConsumerLoop implements Runnable {
    private final ConsumerReaction<String, Object> consumer;
    private final Collection<Class> objectTypes = new LinkedList<>(); // these are the topics
    private QueueListener queueListener;

    public ConsumerLoop(String reactionId,
                        Collection<Class> objectTypes, QueueListener queueListener) {
        this.objectTypes.addAll(objectTypes);
        this.queueListener = queueListener;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", reactionId);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.alkal.kalium.kafka.MultiSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.alkal.kalium.kafka.MultiDeSerializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        HashMap<String, Class> topicToClassMap = new HashMap<>();
        objectTypes.forEach(type -> topicToClassMap.put(type.getSimpleName(), type));
        props.put(Constants.TOPIC_TO_CLASS_MAP, topicToClassMap);

        consumer = new ConsumerReaction<>(props);
        consumer.setReactionId(reactionId);

    }

    @Override
    public void run() {
        try {
            consumer.subscribe(this.objectTypes.stream().map(type -> type.getSimpleName()).collect(Collectors.toList()));
            System.out.println("Start polling for reaction: " + consumer.getReactionId());
            while (true) {
                System.out.println("Reaction: " + consumer.getReactionId() + "  is polling");
                ConsumerRecords<String, ?> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, ?> record : records) {
                    System.out.println("Object arrived for reaction: " + consumer.getReactionId() +
                            ". key: " + record.key() + " topic: " + record.topic() + " content: "+record.value().toString()+" partition: " + record.partition());
                    queueListener.onObjectReceived(consumer.getReactionId(), record.value());
                }
            }
        } catch (Exception e) {
            // ignore for shutdown
            if (e instanceof WakeupException)
                System.out.println("Wakeup exception for reaction: " + consumer.getReactionId());
            else
                e.printStackTrace();
        } finally {
            System.out.println("Shutting down consumer for reaction: " + consumer.getReactionId());
            consumer.close();
        }
    }

    public void shutdown() {

        consumer.wakeup();
    }
}
