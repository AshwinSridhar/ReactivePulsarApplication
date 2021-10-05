package com.reactive.pulsar.ReactivePulsarApplication;

import com.reactive.pulsar.ReactivePulsarApplication.Domain.Message;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class CustomeMessageListener implements MessageListener<Message> {

    public CustomeMessageListener(){
        System.out.println("Constructed the custom message object");
    }

    @Override
    public void received(Consumer<Message> consumer, org.apache.pulsar.client.api.Message<Message> msg) {
        System.out.println("I have received the message");
        System.out.println("Topic - "+consumer.getTopic());
    }

    @Override
    public void reachedEndOfTopic(Consumer<Message> consumer) {
        MessageListener.super.reachedEndOfTopic(consumer);
    }
}
