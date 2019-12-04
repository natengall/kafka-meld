package com.github.meld;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class StreamsTest {

    public static void main(String args[]) throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(new FileInputStream("connect.properties"));
        
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "TheTest");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafkac2n1.dev.bo1.csnzoo.com:9092");
        config.put("auto.offset.reset", "earliest");
        config.put("default.timestamp.extractor", WallclockTimestampExtractor.class);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> testStream = builder.stream("TheTest");
        
        testStream.foreach(new ForeachAction<String, String>() {
            public void apply(String key, String value) {
                try {
                    System.out.println(key+"\t"+value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        
        //streams.close();
    }
}