package com.cem.kafkastreams.controller;

import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Date;

@RestController
@AllArgsConstructor
public class StoreRestController {
    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/readonly/{store}/{key}")
    public String ktable(@PathVariable String store, @PathVariable String key) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, String> pairs = kafkaStreams
                .store(StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.keyValueStore()));
        return pairs.get(key);
    }

    @GetMapping("/timestamp/{store}/{key}")
    public String ktable2(@PathVariable String store, @PathVariable String key) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyWindowStore<Object, Object> pairs = kafkaStreams
                .store(StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.windowStore()));
        String result = "";
        Instant timeFrom = Instant.now().minusSeconds(180); // beginning of time = oldest available
        Instant timeTo = Instant.now(); // now (in processing-time)
        WindowStoreIterator<Object> keyPair = pairs.fetch(key, timeFrom, timeTo);
        while (keyPair.hasNext()) {
            KeyValue<Long, Object> pair = keyPair.next();
            result += new Date(pair.key) + " " + pair.value.toString() + "</br>";
        }
        return result;
    }
}