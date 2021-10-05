package com.reactive.pulsar.ReactivePulsarApplication.Controller;

import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerCache;
import com.reactive.pulsar.ReactivePulsarApplication.Domain.Message;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class IngestController {

    private ReactiveMessageSender<Message> reactiveMessageSender;

    //private ReactiveMessageConsumer<Message> reactiveMessageConsumer;

    public IngestController(ReactivePulsarClient reactivePulsarClient, ReactiveProducerCache reactiveProducerCache){

        reactiveMessageSender = reactivePulsarClient.messageSender(Schema.JSON(Message.class))
                .topic("my-topic1")
                .cache(reactiveProducerCache)
                .build();

        /*reactiveMessageConsumer = reactivePulsarClient.messageConsumer(Schema.JSON(Message.class))
                .topic("my-topic")
                .consumerConfigurer(consumerBuilder -> {
                    consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                            .subscriptionName("my-subscription")
                            .topic("my-topic");
                }).build();*/
    }

    @PostMapping("/postmessage")
    public Mono<Void> sendMessage(@RequestBody Flux<Message> messageFlux){
        Mono<Void> then = reactiveMessageSender.sendMessages(messageFlux.map(MessageSpec::of))
                .then();

        System.out.println("Receiverd messagh");

        return then;
    }
}
