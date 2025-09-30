package com.ouskhouya.kafkaspringcloudstream.controllers;

import com.ouskhouya.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

@RestController
public class PageEventController {


    private final StreamBridge streamBridge;

    @Autowired
    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @GetMapping("/publish")
    public PageEvent send(String name, String topic){
        PageEvent event = new PageEvent(
                name,
                Math.random()>0.5?"U1":"U2",
                new Date(), 10 + new Random().nextInt(1000));
        streamBridge.send(topic, event);
        return event;
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()-> new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"U1":"U2",
                new Date(),
                10+new Random().nextInt(10000)
        );
    }

    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStream(){
        return (stream)->
                stream
                        .filter((k,v)->v.duration()>100)
                        .map((k,v)->new KeyValue<>(v.name(), v.duration()))
                        ;

    }
}
