package com.niketan.kafka.kafkalearning.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niketan.kafka.kafkalearning.model.DriverLocation;
import com.niketan.kafka.kafkalearning.service.KafkPublisherService;


@RestController
@RequestMapping("/kafka")
public class KafkaController {
    
    @Autowired
    private KafkPublisherService kafkaPublisherService;
    
    @PostMapping("/publish")
    public String postMethodName(@RequestBody DriverLocation driverLocation) {
        System.out.println("Received Driver Location: " + driverLocation);
        kafkaPublisherService.publishDriverLocation(driverLocation);
        System.out.println("Published Driver Location to Kafka: " + driverLocation.toString());
        System.out.println("----------------------------------------");
        
        return "Published Successfully";
        
    }
    
}
