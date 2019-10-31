package io.alkal.kalium.kafka.tests;

import io.alkal.kalium.annotations.On;
import io.alkal.kalium.kafka.tests.models.pojo.Payment;

public class MyReaction2 {

    @On
    public void doSomething(Payment payment){
        payment.setProcessed(true);
    }

}
