package io.alkal.kalium.kafka.tests;

import io.alkal.kalium.annotations.On;

public class MyReaction {

    @On
    public void doSomething(Payment payment){
        payment.setProcessed(true);
    }

}
