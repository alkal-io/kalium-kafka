package io.alkal.kalium.kafka.tests;

import io.alkal.kalium.annotations.On;

public class MyReactor3 {

    @On("payment.processed == false")
    public void doSomething(Payment payment){
        payment.setProcessed(true);
    }

}