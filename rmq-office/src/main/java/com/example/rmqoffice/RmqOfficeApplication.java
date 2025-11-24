package com.example.rmqoffice;

import java.util.Map;
import java.util.HashMap;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RmqOfficeApplication {

    private final ObjectMapper jackson;
    private final String MAIN_Q_NAME = "rmq-office_main-queue";
    private final String DEAD_LETTER_Q_NAME = "rmq-office_dead-letter-queue";
    private final String DLQ_ROUTING_KEY = DEAD_LETTER_Q_NAME; // incase of default exchange, the routing key MUST match the queue name

    public RmqOfficeApplication(ObjectMapper jackson) {
        this.jackson = jackson;
    }

    public static void main(String[] args) {
        var context = SpringApplication.run(RmqOfficeApplication.class, args);
        var publisher = context.getBean(Publisher.class); // or move these bean definitions to separate @Configuration class
        publisher.publishMsg(Map.of("message", "Hello RabbitMQ DLQ"));

        // manually start the consumer
//        var app = context.getBean(RmqOfficeApplication.class);
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
            channel.queueDeclare(DEAD_LETTER_Q_NAME, true, false, false, null); // durable=true
            var args = new HashMap<String, Object>();
            args.put("x-dead-letter-exchange", ""); // default exchange
            args.put("x-dead-letter-routing-key", DLQ_ROUTING_KEY);

            channel.queueDeclare(MAIN_Q_NAME, true, false, false, args); // durable=true
            return new Publisher(channel, MAIN_Q_NAME, jackson);
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
            channel.queueDeclare(DEAD_LETTER_Q_NAME, true, false, false, null); // durable=true
            var args = new HashMap<String, Object>();
            args.put("x-dead-letter-exchange", ""); // default exchange
            args.put("x-dead-letter-routing-key", DLQ_ROUTING_KEY);

            channel.queueDeclare(MAIN_Q_NAME, true, false, false, args); // durable=true
            channel.basicQos(1, false);
            var consumer = new Consumer(channel, MAIN_Q_NAME, jackson);
            channel.basicConsume(MAIN_Q_NAME, false, consumer); // autoAck=false -> manual ack
            return consumer;
        } catch (Exception e) {
            System.out.println("caught exception while configuring consumer queue with dead letter queue: " + e.getMessage());
            return null;
        }
    }

    // Drawback: everytime, queue will be deleted (not useful)
    private void deleteExistingQueue(Channel channel, String queueName) {
        try {
//            channel.queueDeclarePassive(queueName); // not needed
            var ifUnused = false;
            var ifEmpty = true;
            channel.queueDelete(queueName, ifUnused, ifEmpty); // if queue not exists, nothing will happen, ifEmpty=true then delete otherwise throws exception
            System.out.println("deleted existing empty queue: " + queueName);
        } catch (Exception e) {
            System.out.println("queue is not empty, not proceeding: " + e.getMessage());
        }
    }

}
