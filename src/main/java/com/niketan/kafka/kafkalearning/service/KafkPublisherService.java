package com.niketan.kafka.kafkalearning.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;


import com.niketan.kafka.kafkalearning.model.DriverLocation;


@Service
public class KafkPublisherService {
    
    
    @Value("${kafka.topic.driver-location}")  
    private String topic;
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    public void publishDriverLocation(DriverLocation driverLocation) {
        
        String key = driverLocation.getDriverId();
        try {
            kafkaTemplate.send(topic, key, driverLocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
