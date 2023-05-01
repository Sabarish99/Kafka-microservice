package com.example.producer.Domain;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Food {
    private  String item;
    private  Double amount;
}
