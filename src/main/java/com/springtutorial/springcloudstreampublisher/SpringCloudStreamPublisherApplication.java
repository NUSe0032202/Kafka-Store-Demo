package com.springtutorial.springcloudstreampublisher;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


//@SpringBootApplication
@Controller
public class SpringCloudStreamPublisherApplication {


	//@Autowired
	//private InteractiveQueryService interactiveQueryService;


	private static KafkaStreams streams = null;

	@PostMapping("/test")
	public void retrieveData(@RequestBody StoreRequest desired) {
		System.out.println("Attempting to retrieve data");
        System.out.println("Request: " + desired.getDesired());
		StoreQueryParameters<ReadOnlyKeyValueStore<String,Long>> storeStoreQueryParameters =
				StoreQueryParameters.fromNameAndType("IncomingRecordCount",
						QueryableStoreTypes.<String,Long>keyValueStore());


		Long output = streams.store(storeStoreQueryParameters).get(desired.getDesired());
		System.out.println("Data count: " + output);
	}

	//To test whether Tomcat is working on port 8080
	@PostMapping("/try")
	public void tryEndPoint () {
		System.out.println("Hit endpoint");
	}

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-event-store");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		//Incoming Topic

		KStream<String,String> textLine = builder.stream("test-event-source");
		KStream<String,String> incoming = textLine.filter((key,value)->value.contains(","))
				.selectKey((key,value)->value.split(",")[0].toLowerCase())
				.mapValues(value->value.split(",")[1].toLowerCase());
		incoming.to("incoming-records");

		//Serdes serializer/deserializer
		Serde<String> stringSerde = Serdes.String();
		Serde<Long> longSerde = Serdes.Long();

		//Transform, getting data from the intermediary topic
		KTable<String,String>  tempTable = builder.table("incoming-records");

        //Aggregation attempt to persist to statestore
		//Second stage: Try to store objects to a state store
		KTable<String,Long> recordTable = tempTable.groupBy((key,value) -> new KeyValue<>(value,value))
						.count(Materialized.<String,Long, KeyValueStore<Bytes, byte[]>>as("IncomingRecordCount")
						.withKeySerde(stringSerde)
						.withValueSerde(longSerde));

		streams = new KafkaStreams(builder.build(), properties);

		streams.start();

		SpringApplication.run(SpringCloudStreamPublisherApplication.class, args);
	}


	//static long tickCount = 0;

	//@Bean
	//public Function<String, String> uppercase() {
	//	return value -> {
		//	System.out.println("Received: " + value);
		//	return value.toUpperCase();
		//};
	//}


	//@Bean
	//@Scheduled(fixedDelay = 5000L)
	//public Supplier<String> stringSupplier() {
		//	return () -> "Hello from supplier KKK123 ";

	//}


	//Getting and mapping the data
	//@Bean
	//public Function<KStream<String,String>,KStream<String,String>> process () {
	//	return input -> input.
		//		transformValues(() -> new ValueTransformerNew(),"Streams-Store");
	//}

	//Declaring a state store as a bean with the name Streams-Store
	//@Bean
	//public StoreBuilder myStore() {
		//return Stores.keyValueStoreBuilder(
		//		Stores.persistentKeyValueStore("Streams-Store-2"),
			//	Serdes.String(),
			//	Serdes.String()
		//);
	//}

	//@Bean
	//public Supplier<String> outputChange () {
    //   return () -> this.store().get("test3");
	    //return () -> "Hello";
	//}

	//private ReadOnlyKeyValueStore<String,String> store() {
	//	return interactiveQueryService.getQueryableStore("Streams-Store", QueryableStoreTypes.<String,String>keyValueStore());
	//}

	//@Bean
	//public StoreBuilder myStore() {
		//return Stores.keyValueStoreBuilder(
		///		Stores.persistentKeyValueStore("Test-1-Store"),
		//		Serdes.String(),
			//	Serdes.String()
		//);
	//}

	//@Bean
	//public Function<KStream<String,String>,KStream<String,String>> testStore () {
	//	return input -> input.
			//	transformValues(() -> new ValueTransformerNew(),"Test-1-Store");
	//}

}
