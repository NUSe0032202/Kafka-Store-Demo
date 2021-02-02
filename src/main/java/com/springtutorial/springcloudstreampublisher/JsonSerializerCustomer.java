package com.springtutorial.springcloudstreampublisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializerCustomer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> tClass;

    public JsonSerializerCustomer(Class<T> customerClass) {
        this.tClass = customerClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, T data) {
        if (data == null)
            return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }
}
