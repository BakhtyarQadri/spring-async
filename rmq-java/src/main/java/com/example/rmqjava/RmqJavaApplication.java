package com.example.rmqjava;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.charset.StandardCharsets;

@SpringBootApplication
public class RmqJavaApplication {

    public static void main(String[] args) {
        SpringApplication.run(RmqJavaApplication.class, args);

        var QUEUE_NAME = "rmq-java_main_queue";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Assuming RabbitMQ is running locally

        // Publisher
        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()
        ) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//            channel.queueDeclare(QUEUE_NAME, true, false, false, null); // durable=true so queue persists
            String message = "Hello, RabbitMQ!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
//            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        } catch (Exception e) {
            System.out.println(e);
        }

        // Consumer
        // don't use try-with-resources bcz it auto-closes connection immediately after starting basicConsume
        // So the consumer never stays alive long enough to actually receive messages
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//            channel.queueDeclare(QUEUE_NAME, true, false, false, null); // durable=true so same queue as publisher

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8); // "UTF-8"
                System.out.println(" [x] Received '" + message + "'");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // manual acknowledgment
            };
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { }); // autoAck=false -> manual ack
        } catch (Exception e) {
            System.out.println(e);
        }

    }

}
