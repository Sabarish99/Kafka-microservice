package com.example.producer.Controller;

import com.example.producer.Domain.Food;
import com.example.producer.Service.FoodService;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("order")
public class FoodController {
    private final FoodService foodService;

    @Autowired
    public FoodController(FoodService foodService) {
        this.foodService = foodService;
    }
    @PostMapping
    public String createFoodOrder(@RequestBody Food food) throws JsonProcessingException
    {
        log.info("Food create order is received ");
        return  foodService.createFood(food);
    }
}
