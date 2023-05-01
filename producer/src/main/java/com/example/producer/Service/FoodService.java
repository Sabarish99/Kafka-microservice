package com.example.producer.Service;

import com.example.producer.Domain.Food;
import com.example.producer.Service.producer.Producer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FoodService {

    private final Producer producer;

    @Autowired
    public FoodService(Producer producer) {
        this.producer = producer;
    }
    public String createFood(Food food) throws JsonProcessingException
    {
        return producer.sendMessage(food);
    }

}
