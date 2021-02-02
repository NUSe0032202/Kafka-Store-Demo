package com.springtutorial.springcloudstreampublisher;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.HashMap;
import java.util.Properties;


@SpringBootApplication
@Controller
public class PublishAndSaveCustomerObjects {


    private static KafkaStreams streams = null;


    //Define two endpoints for storing and retrieving a record
    @PostMapping("/create")
    public void createCustomer(@RequestBody Customer customer) {
        System.out.println("Trying to create a customer....");
        Properties producerProp = new Properties();
        producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Producer<Integer,Customer> producerCustomer = new KafkaProducer<>(producerProp, Serdes.Integer().serializer()
                ,CustomSerdes.Customer().serializer());
        ProducerRecord<Integer,Customer> customerRecord = new ProducerRecord<>("customer-record",customer.getId(),
                customer);
        producerCustomer.send(customerRecord);
    }

    @PostMapping("/retrieve")
    public ResponseEntity<Customer> retrieveCustomer(@RequestBody String id) {
        System.out.println("Attempting to retrieve customer witd id " + id);
        int identifier = Integer.valueOf(id);
        StoreQueryParameters<ReadOnlyKeyValueStore<Integer,Customer>> storeStoreQueryParameters =
                StoreQueryParameters.fromNameAndType("CustomerStore",
                        QueryableStoreTypes.<Integer,Customer>keyValueStore());
        Customer customer = streams.store(storeStoreQueryParameters).get(identifier);
        System.out.println("Retrieved: " + customer.toString());
        return new ResponseEntity<Customer>(customer, HttpStatus.OK);

    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-customer-store");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Init streams builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //Store Builder
        //How to test out concept? : use log compaction ?
        StoreBuilder<KeyValueStore<Integer,Customer>> customerStore = Stores.keyValueStoreBuilder(Stores
                .persistentKeyValueStore("CustomerStore"),Serdes.Integer(),CustomSerdes.Customer())
                .withLoggingEnabled(new HashMap<>());

        //Conversion to table and saving it to a store
        streamsBuilder.table("customer-record", Materialized.<Integer,Customer, KeyValueStore<Bytes, byte[]
                >>as("CustomerStore").withKeySerde(Serdes.Integer()).withValueSerde(CustomSerdes.Customer()));

        streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        SpringApplication.run(PublishAndSaveCustomerObjects.class, args);
    }
}
