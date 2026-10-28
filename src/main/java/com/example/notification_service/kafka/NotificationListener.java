package com.example.notification_service.kafka;

import com.example.notification_service.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    @KafkaListener(topics = "order-created", groupId = "notification-service")
    public void handleOrderCreated(OrderCreatedEvent orderCreatedEvent){
        System.out.println("===EMAIL NOTIFICATION===");
        System.out.println("Email sent to customer: " + orderCreatedEvent.customerId());
        System.out.println("OrderId: " + orderCreatedEvent.orderId());
        System.out.println("Product: " + orderCreatedEvent.product() + " x" + orderCreatedEvent.quantity());
    }
}
