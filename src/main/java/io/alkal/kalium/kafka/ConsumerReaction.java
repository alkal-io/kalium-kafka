package io.alkal.kalium.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

public class ConsumerReaction<K, V> extends KafkaConsumer<K, V> {

    String reactionId = null;


    public ConsumerReaction(Map configs) {
        super(configs);
    }

    public void setReactionId(String reactionId) {
        this.reactionId = reactionId;
    }

    public String getReactionId() {
        return reactionId;
    }
}
