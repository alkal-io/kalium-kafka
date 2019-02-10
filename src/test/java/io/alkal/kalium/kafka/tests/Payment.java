package io.alkal.kalium.kafka.tests;

public class Payment {

    private boolean processed;

    private String id;

    public Payment(){

    }

    public Payment(String id) {
        super();
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
}
