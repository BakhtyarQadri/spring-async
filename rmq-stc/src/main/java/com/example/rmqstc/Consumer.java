package com.example.rmqstc;

import java.util.HashMap;
import java.util.HexFormat;
import com.rabbitmq.client.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import com.fasterxml.jackson.core.JsonProcessingException;

public class Consumer extends DefaultConsumer {

    @Value("${secret-key}")
    private String secretKey;

    private final Channel channel;
    private final String routingKey;
    private final ObjectMapper jackson;

    public Consumer(Channel channel, String routingKey, ObjectMapper jackson) {
        super(channel);
        this.channel = channel;
        this.routingKey = routingKey;
        this.jackson = jackson;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        try {
            var signedMsg = jackson.readValue(body, SignedMsg.class);
            System.out.println("received msg: " + signedMsg);
            var isValidSignature = validateSignature(signedMsg);
            if (!isValidSignature) {
                throw new Exception("signature mismatched");
            }
            if (true) throw new RuntimeException("exception occurred");
            channel.basicAck(envelope.getDeliveryTag(), false); // manual acknowledgment
        } catch (Exception e) {
            System.out.println(e.getMessage());

            try {
                // Get retry count from headers
                var headers = (properties.getHeaders() == null) ? new HashMap<String, Object>() : properties.getHeaders();
                var retryCount = (Integer) headers.getOrDefault("x-retry-count", 0);
                if (retryCount < 3) {
                    retryCount++;
                    headers.put("x-retry-count", retryCount); // upsert
                    var directExchangeName = "";
                    var amqpBasicProperties = new AMQP.BasicProperties.Builder().headers(headers).build(); // .deliveryMode(2) -> persistent
                    channel.basicPublish(directExchangeName, routingKey, amqpBasicProperties, body);
                    channel.basicAck(envelope.getDeliveryTag(), false); // manual acknowledgment for previous msg
                    System.out.println("retry attempt: " + retryCount);
                } else { // Max retries exceeded - send to DLQ
                    channel.basicNack(envelope.getDeliveryTag(), false, false); // requeueMsg (lastParam) = false -> send msg to DLQ if configured or dropped AND requeueMsg=true -> requeue the message
                    System.out.println("sent to dead letter queue");
                }
            } catch (Exception ex) {
                System.out.println(e.getMessage());
            }
        }
    }

    private record SignedMsg<T>(String signature, T unSignedMsg) {}

    private boolean validateSignature(SignedMsg signedMsg) throws JsonProcessingException, NoSuchAlgorithmException {
        return signedMsg.signature.equals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(String.format("%s:%s:%s", jackson.writeValueAsString(signedMsg.unSignedMsg), 1, secretKey).getBytes())));
    }

}
