package com.bxfy.kafka.api.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -21:13
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User implements Serializable {


    String id;
    String name;

    @DateTimeFormat(pattern = "yyyy-MM-dd")
    @JsonFormat(pattern = "yyyy-MM-dd")
    Date date;

    public User(String id, String name){
        this.id = id;
        this.name = name;
    }
}
