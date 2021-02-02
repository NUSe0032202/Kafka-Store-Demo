package com.springtutorial.springcloudstreampublisher;



import lombok.*;

@Getter
@NoArgsConstructor
@Setter
@AllArgsConstructor
@ToString
public class Customer {
     private int id;
     private String first_name;
     private String last_name;
     private long phone_number;
     private String email;
}
