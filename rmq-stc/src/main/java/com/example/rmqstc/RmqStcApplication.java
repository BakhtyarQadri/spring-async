package com.example.rmqstc;

import java.util.Map;
import java.util.HashMap;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RmqStcApplication {

    private final ObjectMapper jackson;
    private final String STC_MAIN_QUEUE_NAME = "rmq-stc_main-queue";
    private final String STC_DLQ_QUEUE_NAME = "rmq-stc_dlq-queue";
    private final String STC_DLQ_ROUTING_KEY = STC_DLQ_QUEUE_NAME; // incase of default exchange, the routing key MUST match the queue name

    public RmqStcApplication(ObjectMapper jackson) {
        this.jackson = jackson;
    }

    public static void main(String[] args) {
        var context = SpringApplication.run(RmqStcApplication.class, args);
        var publisher = context.getBean(Publisher.class); // or move these bean definitions to separate @Configuration class
        publisher.publishMsg(Map.of("message", "Hello RabbitMQ DLQ"));

        // manually start the consumer
//        var app = context.getBean(RmqStcApplication.class);
//        app.consumer(); // manually start the consumer
    }

    @Bean
    public Publisher publisher() {
        try {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost"); // connectionFactory.setUri("amqp://guest:guest@localhost:5672");
            var connection = connectionFactory.newConnection();
            var channel = connection.createChannel();

            // DLQ
            channel.queueDeclare(STC_DLQ_QUEUE_NAME, true, false, false, null); // durable=true
            var args = new HashMap<String, Object>();
            args.put("x-dead-letter-exchange", ""); // default exchange
            args.put("x-dead-letter-routing-key", STC_DLQ_ROUTING_KEY);

            channel.queueDeclare(STC_MAIN_QUEUE_NAME, true, false, false, args); // durable=true
            return new Publisher(channel, STC_MAIN_QUEUE_NAME, jackson);
        } catch (Exception e) {
            System.out.println("caught exception while configuring publisher queue with dead letter queue: " + e.getMessage());
            return null;
        }
    }

    @Bean
    public Consumer consumer() {
        try {
            var connectionFactory = new ConnectionFactory();
            connectionFactory.setUri("amqp://guest:guest@localhost:5672"); // connectionFactory.setHost("localhost");
            var connection = connectionFactory.newConnection();
            var channel = connection.createChannel();

            // DLQ
            channel.queueDeclare(STC_DLQ_QUEUE_NAME, true, false, false, null); // durable=true
            var args = new HashMap<String, Object>();
            args.put("x-dead-letter-exchange", ""); // default exchange
            args.put("x-dead-letter-routing-key", STC_DLQ_ROUTING_KEY);

            channel.queueDeclare(STC_MAIN_QUEUE_NAME, true, false, false, args); // durable=true
            channel.basicQos(1, false);
            var consumer = new Consumer(channel, STC_MAIN_QUEUE_NAME, jackson);
            channel.basicConsume(STC_MAIN_QUEUE_NAME, false, consumer); // autoAck=false -> manual ack
            return consumer;
        } catch (Exception e) {
            System.out.println("caught exception while configuring consumer queue with dead letter queue: " + e.getMessage());
            return null;
        }
    }

}
