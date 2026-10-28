package com.example.notification_service.kafka;

import com.example.notification_service.dto.OrderCreatedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer"
})
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"order-created"})
class NotificationListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    private NotificationListener notificationListener;

    @Test
    @DisplayName("Notification service should consume order-created event independently")
    void shouldConsumeOrderCreatedEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                2002L,
                "Keyboard",
                1,
                LocalDateTime.now()
        );

        kafkaTemplate.send("order-created", event);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                verify(notificationListener).handleOrderCreated(any(OrderCreatedEvent.class))
        );
    }
}