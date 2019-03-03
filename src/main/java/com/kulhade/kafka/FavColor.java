package com.kulhade.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavColor {

    public static void main(String... args){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-favColor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> kStream = builder.stream("color-input");

            kStream.filter((key,value) -> value.contains(":"))
                .selectKey((key,value)->value.split(":")[0].toLowerCase())
                .mapValues(value->value.split(":")[1].toLowerCase())
                .filter((key,value)->Arrays.asList("green","red","blue").contains(value))
                .to("color-intermediate" ,Produced.with(Serdes.String(),Serdes.String()));
        KTable<String,String> kTable = builder.table("color-intermediate");
            kTable.groupBy((user, color) -> new KeyValue<>(color,color))
                    .count("color-count-store")
                    .to(Serdes.String(), Serdes.Long(),"color-count-output");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }
}
