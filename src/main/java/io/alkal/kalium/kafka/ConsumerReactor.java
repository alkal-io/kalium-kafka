package io.alkal.kalium.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

public class ConsumerReactor<K, V> extends KafkaConsumer<K, V> {

    String reactorId = null;


    public ConsumerReactor(Map configs) {
        super(configs);
    }

    public void setReactorId(String reactorId) {
        this.reactorId = reactorId;
    }

    public String getReactorId() {
        return reactorId;
    }
}
