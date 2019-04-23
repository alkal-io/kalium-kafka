package io.alkal.kalium.kafka.tests;

import java.util.UUID;

public class Receipt {

    private boolean processed;

    private String id;

    public Receipt(){
        id = UUID.randomUUID().toString();
    }

    public Receipt(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "{id: "+id+"}";
    }
}
