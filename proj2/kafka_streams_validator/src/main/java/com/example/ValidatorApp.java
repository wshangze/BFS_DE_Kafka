package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

public class ValidatorApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-validator-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("bf_employee_cdc");

        ObjectMapper mapper = new ObjectMapper();

        KStream<String, String>[] branches = source.branch(
            (key, value) -> {
                try {
                    Employee e = mapper.readValue(value, Employee.class);
                    return Validator.isValid(e);
                } catch (Exception ex) {
                    return false;
                }
            },
            (key, value) -> true
        );

        branches[0].to("bf_employee_cdc_valid");
        branches[1].to("bf_employee_cdc_dlq");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
