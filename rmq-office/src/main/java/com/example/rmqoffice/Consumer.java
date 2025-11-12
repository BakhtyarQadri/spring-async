package com.example.rmqoffice;

import java.io.IOException;
import java.util.HashMap;
import java.util.HexFormat;
import com.rabbitmq.client.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
            var signedMsg = jackson.readValue(body, SignedMsg.class);
            System.out.println("received msg: " + signedMsg);
            var isValidSignature = validateSignature(signedMsg);
            if (!isValidSignature) {
                throw new Exception("signature mismatched");
            }
//            if (true) return;
//            throw new Exception("exception occurred");
            CompletableFuture.runAsync(() -> runAsyncFlow(properties.getHeaders(), body, envelope.getDeliveryTag()) );
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            channel.basicAck(envelope.getDeliveryTag(), false);
            System.out.println("msg acknowledged");
        }
    }

    private void runAsyncFlow(Map<String, Object> msgHeaders, byte[] body, Long deliveryTag) {
        try {
//            if (true) return;
            throw new Exception("exception occurred");
//            System.out.println("msg processed successfully");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            try {
                // Get retry count from headers
                var headers = (msgHeaders == null) ? new HashMap<String, Object>() : msgHeaders;
                var retryCount = (Integer) headers.getOrDefault("x-retry-count", 0);
                if (retryCount < 3) {
                    retryCount++;
                    headers.put("x-retry-count", retryCount); // upsert
                    var directExchangeName = "";
                    var amqpBasicProperties = new AMQP.BasicProperties.Builder()
                            .contentType("text/plain")
                            .deliveryMode(2)
                            .headers(headers)
                            .build();
                    channel.basicPublish(directExchangeName, routingKey, amqpBasicProperties, body);
                    System.out.println("retry attempt: " + retryCount);
                } else { // Max retries exceeded - send to DLQ
                    channel.basicNack(deliveryTag, false, false); // requeueMsg (lastParam) = false -> send msg to DLQ if configured or dropped AND requeueMsg=true -> requeue the message
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
