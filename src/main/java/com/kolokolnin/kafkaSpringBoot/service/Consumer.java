package com.kolokolnin.kafkaSpringBoot.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {

    @KafkaListener(topics = "mytopic", groupId = "groupId")
    public void consumFromTopic(String message){
        System.out.println("Consumed message " + message);
    }

}
