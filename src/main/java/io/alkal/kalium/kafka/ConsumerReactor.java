package io.alkal.kalium.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Map;

public class ConsumerReactor<K, V> extends KafkaConsumer<K, V> {

    Class<?> reactor = null;


    public ConsumerReactor(Map configs) {
        super(configs);
    }

    public void setReactor(Class<?> reactor) {
        this.reactor = reactor;
    }

    public Class<?> getReactor() {
        return reactor;
    }
}
