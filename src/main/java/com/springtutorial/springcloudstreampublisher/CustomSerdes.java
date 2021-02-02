package com.springtutorial.springcloudstreampublisher;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class CustomSerdes {

    static public final class CustomerSerde extends Serdes.WrapperSerde<Customer> {
        public CustomerSerde() {
            super(new JsonSerializerCustomer<>(Customer.class)
                    ,new JsonDeserializerCustomer<>(Customer.class));
        }
    }

    public static Serde<Customer> Customer() {
        return new CustomSerdes.CustomerSerde();
    }
}
