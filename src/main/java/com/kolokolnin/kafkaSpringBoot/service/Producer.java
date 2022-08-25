package com.kolokolnin.kafkaSpringBoot.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class Producer {

    public static final String topicName = "mytopic";

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    public void publishToTopic(String message){
        System.out.println("Publishing to topic "+topicName);
        this.kafkaTemplate.send(topicName,message);
    }

}
