package com.reactive.pulsar.ReactivePulsarApplication;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.reactive.pulsar.ReactivePulsarApplication.Domain.Message;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

import java.time.Duration;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReactivePulsarApplicationTests {

	@Autowired
	ReactivePulsarClient reactivePulsarClient;

	@Autowired
	WebTestClient webTestClient;

	@Test
	void ShouldLoadMessages() {

		webTestClient.post()
				.uri("/postmessage")
				.bodyValue(new Message("Ashwin",33))
				.exchange()
				.expectStatus()
				.isOk();
	}

	@Test
	void shouldReceiveMessage(){
		ReactiveMessageConsumer<Message> messageConsumer = reactivePulsarClient.messageConsumer(Schema.JSON(Message.class))
				.consumerConfigurer(consumerBuilder -> {
					consumerBuilder.subscriptionName("my-subscription1")
							.topic("my-topic1")
							.subscriptionInitialPosition(SubscriptionInitialPosition.Latest);
				})
				.acknowledgeAsynchronously(false)
				.build();

		messageConsumer.consumeNothing().block();

		messageConsumer.consumeMessage(messageMono -> messageMono.map(MessageResult::acknowledgeAndReturn))
				.as(StepVerifier::create)
				.assertNext(message -> {
					Message consumedMessage = message.getValue();
					Assertions.assertThat(consumedMessage.getName()).isEqualTo("Ashwin");
					Assertions.assertThat(consumedMessage.getAge()).isEqualTo(33);
				})
				.thenCancel();
				//.verify(Duration.ofSeconds(5));
	}

}
