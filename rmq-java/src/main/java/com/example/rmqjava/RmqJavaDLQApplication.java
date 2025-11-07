package com.example.rmqjava;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RmqJavaDLQApplication {

    private static final String MAIN_EXCHANGE = "main-exchange";
    private static final String MAIN_QUEUE = "main-queue";
    private static final String MAIN_ROUTING_KEY = "main.routing.key";

    private static final String DLQ_EXCHANGE = "dlq-direct-exchange";
    private static final String DLQ_QUEUE = "dlq-queue";
    private static final String DLQ_ROUTING_KEY = "dlq.routing.key";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 1️⃣ DLQ Exchange, Queue, Binding
        channel.exchangeDeclare(DLQ_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.queueDeclare(DLQ_QUEUE, true, false, false, null);
        channel.queueBind(DLQ_QUEUE, DLQ_EXCHANGE, DLQ_ROUTING_KEY);

        // 2️⃣ DLQ configuration for Main Queue
        Map<String, Object> argsMap = new HashMap<>();
        argsMap.put("x-dead-letter-exchange", DLQ_EXCHANGE);
        argsMap.put("x-dead-letter-routing-key", DLQ_ROUTING_KEY);

        // Main Exchange, Queue, Binding
        channel.exchangeDeclare(MAIN_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.queueDeclare(MAIN_QUEUE, true, false, false, argsMap);
        channel.queueBind(MAIN_QUEUE, MAIN_EXCHANGE, MAIN_ROUTING_KEY);

        // 3️⃣ Publish a message
        String message = "Hello DLQ World!";
        channel.basicPublish(MAIN_EXCHANGE, MAIN_ROUTING_KEY, null, message.getBytes());
        System.out.println("Sent: " + message);

        // 4️⃣ Consumer that intentionally fails
        DeliverCallback deliverCallback = (tag, delivery) -> {
            String msg = new String(delivery.getBody());
            System.out.println("Received: " + msg);
            try {
                if (true) throw new RuntimeException("exception occurred");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception ex) {
                System.out.println("Error: " + ex.getMessage());
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false); // requeueMsg (lastParam) = false -> send msg to DLQ if configured or dropped AND requeueMsg=true -> requeue the message
                System.out.println("Sent to Dead Letter Queue");
            }
        };

        channel.basicConsume(MAIN_QUEUE, false, deliverCallback, tag -> {}); // autoAck=false -> manual ack
    }
}
