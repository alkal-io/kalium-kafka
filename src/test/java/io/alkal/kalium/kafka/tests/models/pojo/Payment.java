package io.alkal.kalium.kafka.tests.models.pojo;

import java.util.UUID;

public class Payment {

    private boolean processed;

    private String id;

    public Payment(){
        id = UUID.randomUUID().toString();
    }

    public Payment(String id) {
        this.id = id;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "{id: "+id+", processed: "+processed+"}";
    }
}
