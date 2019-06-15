package com.github.meld;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.Collator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

@Path("connect")
@Produces(MediaType.APPLICATION_JSON)
public class ConnectController {

    LinkedList<String> connectClusters = new LinkedList<String>();
    private HashMap<String, HashMap<String, String>> offsets = new HashMap<String, HashMap<String, String>>();
    private HashMap<String, HashMap<String, String>> configs = new HashMap<String, HashMap<String, String>>();

    Properties prop = new Properties();

    public ConnectController() throws FileNotFoundException, IOException {

        prop.load(new FileInputStream("connect.properties"));
        prop.keySet().stream().forEach(key -> {
            String keyString = key.toString();
            if (keyString.endsWith(".bootstrap_servers")) {
                String clusterName = keyString.replaceAll(".bootstrap_servers", "");
                if (prop.containsKey(clusterName + ".offset") && prop.containsKey(clusterName + ".config")
                        && prop.containsKey(clusterName + ".url")) {
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
            offsets.put(connectCluster, new HashMap<String, String>());
            configs.put(connectCluster, new HashMap<String, String>());

            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "meld-" + System.currentTimeMillis());
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty(connectCluster + ".bootstrap_servers"));
            config.put("auto.offset.reset", "earliest");
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);
            StreamsBuilder builder = new StreamsBuilder();

            // keep offset caches up to date
            KStream<String, String> offsetsStream = builder.stream(prop.getProperty(connectCluster + ".offset"));
            offsetsStream.foreach(new ForeachAction<String, String>() {
                public void apply(String key, String value) {
                    try {
                        offsets.get(connectCluster).put(key, value);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            // keep config caches up to date
            KStream<String, String> configsStream = builder.stream(prop.getProperty(connectCluster + ".config"));
            configsStream.foreach(new ForeachAction<String, String>() {
                public void apply(String key, String value) {
                    try {
                        configs.get(connectCluster).put(key, value);
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

    @GET
    public Response getClusters() {
        return Response.ok(connectClusters).build();
    }

    @GET
    @Path("/{cluster}")
    public Response getConnectors(@PathParam("cluster") String cluster) throws IOException {
        try {
            Stream<Object> connectors = configs.get(cluster).keySet().stream().filter(e -> e.startsWith("connector-"))
                    .map(e -> e.substring(10));
            return Response.ok(connectors).build();
        } catch (NullPointerException npe) {
            return Response.ok("Error: [" + cluster + "] is not a valid Connect cluster.").build();
        }
    }

    @GET
    @Path("/{cluster}/{connector}")
    public Response getConnector(@PathParam("cluster") String cluster, @PathParam("connector") String connector)
            throws IOException {
        String properties = configs.get(cluster).get("connector-" + connector);
        if (properties == null) {
            return Response
                    .ok("Error: Connector [" + connector + "] does not exist on Connect cluster [" + cluster + "].")
                    .build();
        }
        return Response.ok(properties).build();
    }

    @GET
    @Path("/{cluster}/{connector}/offset")
    public Response getOffset(@PathParam("cluster") String cluster, @PathParam("connector") String connector)
            throws IOException {
        return Response
                .ok(offsets.get(cluster).entrySet().stream().filter(s -> s.getKey().contains(connector)).collect(
                        Collectors.toMap(e -> e.getKey().substring(e.getKey().indexOf(",") + 1), e -> e.getValue())))
                .build();
    }

    @GET
    @Path("/{cluster}/{connector}/offset/{offsetKey}")
    public Response getOffsetKey(@PathParam("cluster") String cluster, @PathParam("connector") String connector,
            @PathParam("offsetKey") String offsetKey) throws IOException {
        return Response.ok(offsets.get(cluster).entrySet().stream()
                .filter(s -> s.getKey().contains(connector) && s.getKey().contains(offsetKey))
                .collect(Collectors.toMap(e -> e.getKey().substring(e.getKey().indexOf(",") + 1), e -> e.getValue())))
                .build();
    }

    @PUT
    @Path("/{cluster}/{connector}/offset/{offsetKey}/{offset}")
    public Response setOffset(@PathParam("cluster") String cluster, @PathParam("connector") String connector,
            @PathParam("offsetKey") String offsetKey, @PathParam("offset") long offset) throws IOException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty(cluster + ".bootstrap_servers"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "meld-" + System.currentTimeMillis());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        offsets.get(cluster).entrySet().stream()
                .filter(s -> s.getKey().contains(connector) && s.getKey().contains(offsetKey)).forEach(e -> {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                            prop.getProperty(cluster + ".offset"), e.getKey(),
                            e.getValue().replaceAll(":[0-9]*", ":" + offset));
                    producer.send(record, (metadata, exception) -> {
                        if (metadata == null) {
                            exception.printStackTrace();
                        } else {
                        }
                    });
                });

        producer.close();
        sendPostRequest(prop.getProperty(cluster + ".url") + "/connectors/" + connector + "/tasks/0/restart");
        return Response.ok().build();

    }

    private Response sendPostRequest(String url) throws IOException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        return invocationBuilder.post(Entity.entity("", MediaType.APPLICATION_JSON));
    }
}