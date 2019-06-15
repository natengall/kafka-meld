package com.github.meld;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimestamp) {
        final long timestamp = consumerRecord.timestamp();

        if (timestamp < 0) {
            return System.currentTimeMillis();
        }

        return timestamp;
    }
}