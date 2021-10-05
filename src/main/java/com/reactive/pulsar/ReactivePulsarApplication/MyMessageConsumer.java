package com.reactive.pulsar.ReactivePulsarApplication;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.spring.PulsarReactiveAdapterAutoConfiguration;
import com.reactive.pulsar.ReactivePulsarApplication.Domain.Message;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class MyMessageConsumer {

    private ReactiveMessageConsumer<Message> reactiveMessageConsumer;

    @Autowired
    private ReactivePulsarClient reactivePulsarClient;

    @Autowired
    private CustomeMessageListener customeMessageListener;

    @EventListener(ApplicationReadyEvent.class)
    public void receiveMessage(){

        reactiveMessageConsumer = reactivePulsarClient.messageConsumer(Schema.JSON(Message.class))
                .topic("my-topic")
                .consumerConfigurer(consumerBuilder -> {
                    try {
                        consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                                .subscriptionName("my-subscription")
                                .topic("my-topic")
                                .messageListener(customeMessageListener)
                                .subscribe();
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                }).build();

        System.out.println("Constructed the messageConsumer");
    }
}
