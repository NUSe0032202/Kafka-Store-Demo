package com.springtutorial.springcloudstreampublisher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.Properties;
import java.util.function.Consumer;


@SpringBootApplication
@Controller
public class ProcessorApproach {

    KeyValueStore<Integer,Customer> store = null;

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @PostMapping("/getRecord")
    public ResponseEntity<Customer> getDesired(@RequestBody String desired) {
        ReadOnlyKeyValueStore<Integer,Customer> store = null;
        store =  interactiveQueryService
                .getQueryableStore("custom-store", QueryableStoreTypes.<Integer, Customer>keyValueStore());
        if(store == null) {
            System.out.println("State store is null");
            System.exit(0);
        } else {
            System.out.println("Exit fine");
        }
        Customer target = store.get(Integer.valueOf(desired));
        return new ResponseEntity<Customer>(target, HttpStatus.OK);
    }

    @PostMapping("/create")
    public void createCustomerNew (@RequestBody Customer customer) {
        System.out.println("Trying to create a customer....");
        Properties producerProp = new Properties();
        producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Producer<Integer,Customer> producerCustomer = new KafkaProducer<>(producerProp, Serdes.Integer().serializer()
                ,CustomSerdes.Customer().serializer());
        ProducerRecord<Integer,Customer> customerRecord = new ProducerRecord<>("customerRecordProcessor",customer.getId(),
                customer);
        producerCustomer.send(customerRecord);
    }

    @PostMapping("/getAll")
    public ResponseEntity<ArrayList<Customer>> retrieveAll() {
        ArrayList<Customer> list = new ArrayList<>();
        System.out.println("Attempting to retrieve all customers");
        KeyValueIterator<Integer,Customer> keyValueIterator = interactiveQueryService
                .getQueryableStore("custom-store", QueryableStoreTypes.<Integer, Customer>keyValueStore()).all();
        while(keyValueIterator.hasNext()) {
            Customer customer = keyValueIterator.next().value;
            list.add(customer);
        }
        return new ResponseEntity<ArrayList<Customer>>(list,HttpStatus.OK);
    }


    public static void main(String[] args) {
        SpringApplication.run(ProcessorApproach.class, args);
    }


    //To be able to use interactive query you have to declare a store as a bean
    @Bean
    public StoreBuilder myStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("custom-store"), Serdes.Integer(),
                CustomSerdes.Customer()
        );
    }

    @Bean
    public Consumer<KStream<Integer,Customer>> customerRecordProcessor () {
        return input -> input.process((ProcessorSupplier<Integer,Customer>) () -> new Processor<Integer, Customer>() {
            @Override
            public void init(ProcessorContext processorContext) {
                System.out.println("Processor init");
                store = (KeyValueStore<Integer, Customer>) processorContext.getStateStore("custom-store");
            }

            @Override
            public void process(Integer integer, Customer customer) {
                Customer temp = store.get(integer);
                System.out.println("In the processing stage");
                if(temp == null) {
                    store.put(integer,customer);
                } else {
                    //If a record exists than update it
                    System.out.println("Update!!!!!!");
                    temp.setEmail(customer.getEmail());
                    temp.setFirst_name(customer.getFirst_name());
                    temp.setLast_name(customer.getLast_name());
                    temp.setPhone_number(customer.getPhone_number());
                    store.put(integer,temp);
                }
                System.out.println("Done with the processing stage");
            }

            @Override
            public void close() {

            }
        },"custom-store");
    }
}
