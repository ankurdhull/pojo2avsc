package com.discc.avro.model;

import lombok.Value;

@Value
public class User {
    String name;
    String email;
    String phone;
    int age;
}
