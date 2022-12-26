package com.cem.kafkastreams.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ReduceExample {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final String INPUT_TOPIC = "reduce-input-topic";
    private static final String OUTPUT_TOPIC = "reduce-output-topic";

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));
        Reducer<String> reducer = (String val1, String val2) -> val1 + val2;

        messageStream
                .peek((key, val) -> System.out.println("1. Step key: " + key + ", val: " + val))
                .mapValues(val -> val.substring(3))
                .peek((key, val) -> System.out.println("2. Step key: " + key + ", val: " + val))
                .filter((key, value) -> Long.parseLong(value) > 1)
                .peek((key, val) -> System.out.println("3. Step key: " + key + ", val: " + val))
                .groupByKey()
                .reduce(reducer)
                .toStream()
                .peek((key, val) -> System.out.println("4. Step key: " + key + ", val: " + val))
                .to(OUTPUT_TOPIC, Produced.with(STRING_SERDE, STRING_SERDE));
    }
}