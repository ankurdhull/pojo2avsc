package com.discc.avro.model.sub;

import com.discc.avro.model.User;
import lombok.NonNull;
import lombok.Value;

import java.util.Collection;

@Value
public class Department {
    @NonNull
    Collection<User> users;
    String name;
    String address;
    User manager;
}
