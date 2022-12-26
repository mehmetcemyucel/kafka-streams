package com.cem.kafkastreams.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class SessionExample {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final String INPUT_TOPIC = "session-input-topic";

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));
        Reducer<String> reducer = (String val1, String val2) -> val1 + val2;
        Duration sessionDuration = Duration.ofMinutes(3);
        SessionWindows sessionWindow = SessionWindows.ofInactivityGapWithNoGrace(sessionDuration);

        messageStream
                .peek((key, val) -> System.out.println("1. Step key: " + key + ", val: " + val))
                .groupByKey()
                .windowedBy(sessionWindow)
                .reduce(reducer, Materialized.as("session"))
                .toStream()
                .peek((key, val) -> System.out.println("2. Step key: " + key.key() + " "
                        + key.window().startTime().toString() + " - " + key.window().endTime().toString() + ", val: " + val));
    }
}