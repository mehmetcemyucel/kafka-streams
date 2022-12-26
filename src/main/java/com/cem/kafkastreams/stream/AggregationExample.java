package com.cem.kafkastreams.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AggregationExample {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final String INPUT_TOPIC = "aggregation-input-topic";
    private static final String OUTPUT_TOPIC = "aggregation-output-topic";

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));
        Aggregator<String, String, Long> aggregator = (key, value, sum) -> Long.parseLong(value) + sum;
        Initializer<Long> initializer = () -> 0L;

        messageStream
                .groupByKey()
                .aggregate(initializer, aggregator, Materialized.with(STRING_SERDE, LONG_SERDE))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(STRING_SERDE, LONG_SERDE));
    }
}