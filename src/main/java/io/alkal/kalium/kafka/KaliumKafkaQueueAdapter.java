package io.alkal.kalium.kafka;

import io.alkal.kalium.interfaces.KaliumQueueAdapter;
import io.alkal.kalium.internals.QueueListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class KaliumKafkaQueueAdapter implements KaliumQueueAdapter {

    private static final long POST_INIT_WARM_TIME = 500L;

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private Properties kafkaProps;

    private QueueListener queueListener;
    private ExecutorService postingExecutorService;
    private ExecutorService consumersExecutorService;
    private List<ConsumerLoop> consumers;

    public KaliumKafkaQueueAdapter(String kafkaEndpoint) {
        kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS, kafkaEndpoint);
        kafkaProps.put("acks", "all");
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("buffer.memory", 33554432);

        //TODO provide kalium implementation for these ones
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "io.alkal.kalium.kafka.JsonSerializer");

    }

    @Override
    public void start() {
        Collection<String> reactionIds = this.queueListener.getReactionIdsToObjectTypesMap().keySet();
        if (reactionIds != null && reactionIds.size() > 0) {
            consumersExecutorService = Executors.newFixedThreadPool(reactionIds.size());
            consumers = queueListener.getReactionIdsToObjectTypesMap().entrySet().stream().map(reaction ->
                    new ConsumerLoop(reaction.getKey(), reaction.getValue(), this.queueListener)
            ).collect(Collectors.toList());
            consumers.forEach(consumer -> consumersExecutorService.submit(consumer));

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    shutdownConsumerLoops();
                }
            });
        }
        postingExecutorService = Executors.newFixedThreadPool(10);
        try {
            Thread.sleep(POST_INIT_WARM_TIME);
        } catch (InterruptedException e) {
            //do nothing TODO: log event
        }

    }


    @Override
    public void post(Object o) {
        postingExecutorService.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("Object is about to be sent. [ObjectType=" + o.getClass().getSimpleName() + "], [content=" + o.toString() + "]");
                try {
                    Producer<String, Object> producer = new KafkaProducer<>(kafkaProps);
                    producer.send(new ProducerRecord<String, Object>(o.getClass().getSimpleName(), o));

                    producer.close();
                    System.out.println("Object sent");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    @Override
    public void setQueueListener(QueueListener queueListener) {
        this.queueListener = queueListener;
    }

    @Override
    public void stop() {
        postingExecutorService.shutdown();
        shutdownConsumerLoops();

    }

    private void shutdownConsumerLoops() {
        if (consumers != null) {
            for (ConsumerLoop consumer : consumers) {
                consumer.shutdown();
            }
            consumersExecutorService.shutdown();
            try {
                consumersExecutorService.awaitTermination(5000, TimeUnit.DAYS.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumers = null;
        }
    }

}
