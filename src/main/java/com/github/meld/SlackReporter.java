package com.github.meld;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.text.Collator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.seratch.jslack.Slack;
import com.github.seratch.jslack.api.webhook.Payload;
import com.wordnik.swagger.annotations.Api;

@Path("connect")
@Produces(MediaType.APPLICATION_JSON)
@Api(value="/connect", description="Operations on the hello object")
public class SlackReporter {

    LinkedList<String> connectClusters = new LinkedList<String>();

    Properties prop = new Properties();

    public SlackReporter() throws FileNotFoundException, IOException {

        prop.load(new FileInputStream("connect.properties"));
        prop.keySet().stream().forEach(key -> {
            String keyString = key.toString();
            if (keyString.endsWith(".bootstrap_servers")) {
                String clusterName = keyString.replaceAll(".bootstrap_servers", "");
                if (prop.containsKey(clusterName + ".offset") && prop.containsKey(clusterName + ".config")) {
                    connectClusters.add(clusterName);
                }
            }
        });
        connectClusters.sort(new Comparator<String>() {
            public int compare(String o1, String o2) {
                return Collator.getInstance().compare(o1, o2);
            }
        });

        for (String connectCluster : connectClusters) {

            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "meld-slack-" + System.currentTimeMillis());
            //config.put(StreamsConfig.APPLICATION_ID_CONFIG, "meld");
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty(connectCluster + ".bootstrap_servers"));
            config.put("auto.offset.reset", "latest");
            config.put("default.timestamp.extractor", WallclockTimestampExtractor.class);
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            StreamsBuilder builder = new StreamsBuilder();

           
            // report on config updates
            KStream<String, String> configStream = builder.stream(prop.getProperty(connectCluster + ".config"));
            configStream.foreach(new ForeachAction<String, String>() {
                public void apply(String key, String value) {
                    try {
                        if (prop.containsKey("slack.webhook") && key.startsWith("connector-")) {
                            sendSlackMessage("*" + key.substring(10) + "* has been updated on `" + connectCluster + "`:```" + value + "```");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            // report on status updates
            KStream<String, String> statusStream = builder.stream(prop.getProperty(connectCluster + ".status"));
            statusStream.foreach(new ForeachAction<String, String>() {
                public void apply(String key, String value) {
                    try {
                        if (prop.containsKey("slack.webhook") && key.startsWith("status-task-")) {
                            sendSlackMessage("Task *" + key.substring(12) + "* status has been updated on `" + connectCluster + "`:```" + value + "```");
                        }
                        if (prop.containsKey("slack.webhook") && key.startsWith("status-connector-")) {
                            sendSlackMessage("Connector *" + key.substring(17) + "* status has been updated on `" + connectCluster + "`:```" + value + "```");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            // start Kafka Streams
            final KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.cleanUp();
            streams.start();
        }
    }

    private void sendSlackMessage(String msg) {
        Payload payload = Payload.builder()
                .text(msg)
                .build();
        Slack slack = Slack.getInstance();
        try {
            slack.send(prop.getProperty("slack.webhook"), payload);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}