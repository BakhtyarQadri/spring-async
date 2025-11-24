package com.example.rmqoffice;

import java.util.HexFormat;
import java.security.MessageDigest;
import com.rabbitmq.client.Channel;
import java.security.NoSuchAlgorithmException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import com.fasterxml.jackson.core.JsonProcessingException;

//@Component
public class Publisher {

    @Value("${secret-key}")
    private String secretKey;

    private final Channel channel;
    private final String routingKey;
    private final ObjectMapper jackson;

    public Publisher(Channel channel, String routingKey, ObjectMapper jackson) {
        this.channel = channel;
        this.routingKey = routingKey;
        this.jackson = jackson;
    }

    public <T> void publishMsg(T unSignedMsg) {
        try {
            var directExchangeName = "";
            var signedMsg = getSignedMsg(unSignedMsg);
            channel.basicPublish(directExchangeName, routingKey, null, jackson.writeValueAsBytes(signedMsg)); // MessageProperties.PERSISTENT_TEXT_PLAIN
            System.out.println("sent msg: " + signedMsg);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // msg with "retry" header
//    public <T> void publishMsg(T unSignedMsg) {
//        try {
//            var directExchangeName = "";
//            Map<String, Object> headers = new HashMap<>();
//            headers.put("x-retry-count", 0); // Initialize retry count
//            var props = MessageProperties.PERSISTENT_TEXT_PLAIN.builder().headers(headers).build();
//            channel.basicPublish(directExchangeName, routingKey, props, jackson.writeValueAsBytes(unSignedMsg));
//            System.out.println("sent msg: " + unSignedMsg);
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//    }

    private <T> SignedMsg getSignedMsg(T unSignedMsg) throws JsonProcessingException, NoSuchAlgorithmException {
        var signature = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(String.format("%s:%s:%s", jackson.writeValueAsString(unSignedMsg), 1, secretKey).getBytes()));
        return new SignedMsg(signature, unSignedMsg);
    }

    private record SignedMsg<T>(String signature, T unSignedMsg) {}

}
