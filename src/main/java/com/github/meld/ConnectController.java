package com.github.meld;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.Collator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
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
public class ConnectController {

    LinkedList<String> connectClusters = new LinkedList<String>();
    private HashMap<String, HashMap<String, String>> offsetsMap = new HashMap<String, HashMap<String, String>>();
    private HashMap<String, HashMap<String, String>> configsMap = new HashMap<String, HashMap<String, String>>();
    private HashMap<String, HashMap<String, HashMap<String, String>>> statusesMap = new HashMap<String, HashMap<String, HashMap<String, String>>>();

    //TEMP
    private HashMap<String, HashMap<String, HashMap<String, HashMap<String, HashMap<String, String>>>>> clusterToServerToDatabaseToConnector = new HashMap<String, HashMap<String, HashMap<String, HashMap<String, HashMap<String, String>>>>>();
    // --temp

    Properties prop = new Properties();

    public ConnectController() throws FileNotFoundException, IOException {

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
            offsetsMap.put(connectCluster, new HashMap<String, String>());
            configsMap.put(connectCluster, new HashMap<String, String>());
            statusesMap.put(connectCluster, new HashMap<String, HashMap<String, String>>());

            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "meld-" + System.currentTimeMillis());
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty(connectCluster + ".bootstrap_servers"));
            config.put("auto.offset.reset", "earliest");
            config.put("default.timestamp.extractor", WallclockTimestampExtractor.class);
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            StreamsBuilder builder = new StreamsBuilder();

            // keep offset caches up to date
            KStream<String, String> offsetsStream = builder.stream(prop.getProperty(connectCluster + ".offset"));
            offsetsStream.foreach(new ForeachAction<String, String>() {
                public void apply(String key, String value) {
                    try {
                        offsetsMap.get(connectCluster).put(key, value);
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
                        configsMap.get(connectCluster).put(key, value);
                        
                        //---TEMP---
                        try {
                            //keep tablesByCluster up to date
                            if(key != null && key.startsWith("connector-") && value != null && value.contains("JdbcSourceConnector")) {
                                JsonNode json = new ObjectMapper().readTree(value);
                                ObjectNode obj = (ObjectNode) json.get("properties");
                                String connectionUrl = obj.get("connection.url").asText();
                                if (clusterToServerToDatabaseToConnector.get(connectCluster)== null) {
                                    clusterToServerToDatabaseToConnector.put(connectCluster, new HashMap<String, HashMap<String, HashMap<String, HashMap<String, String>>>>());
                                }
                                Pattern serverPattern = Pattern.compile("(?<=://)(.*?)(?=;)");
                                Matcher serverMatcher = serverPattern.matcher(connectionUrl);
                                if (serverMatcher.find()) {
                                    String databaseServer = serverMatcher.group(0);
                                    if (clusterToServerToDatabaseToConnector.get(connectCluster).get(databaseServer)== null) {
                                        clusterToServerToDatabaseToConnector.get(connectCluster).put(databaseServer, new HashMap<String, HashMap<String, HashMap<String, String>>>());
                                    }
                                    Pattern databasePattern = Pattern.compile("(?<=databaseName=)(.*?)(?=;)");
                                    Matcher databaseMatcher = databasePattern.matcher(connectionUrl);
                                    if (databaseMatcher.find()) {
                                        String database = databaseMatcher.group(0);
                                        if (clusterToServerToDatabaseToConnector.get(connectCluster).get(databaseServer).get(database)== null) {
                                            clusterToServerToDatabaseToConnector.get(connectCluster).get(databaseServer).put(database, new HashMap<String, HashMap<String, String>>());
                                        }
                                        HashMap<String, String> connectorDetails = new HashMap<String, String>();
                                        connectorDetails.put("mode", obj.get("mode").asText());
                                        if (obj.get("query") != null) {
                                            connectorDetails.put("query", obj.get("query").asText().replaceAll("( )+", " "));
                                        } else if (obj.get("table.whitelist") != null) {
                                            connectorDetails.put("table.whitelist", obj.get("table.whitelist").asText());
                                        }
                                        clusterToServerToDatabaseToConnector.get(connectCluster).get(databaseServer).get(database).put(key.substring(10), connectorDetails);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //---/TEMP---
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            
            // keep status caches up to date
            KStream<String, String> statusesStream = builder.stream(prop.getProperty(connectCluster + ".status"));
            
            statusesStream.foreach(new ForeachAction<String, String>() {
                public void apply(String key, String value) {
                    try {
                        if (value != null && !value.contains("UNASSIGNED") && key.startsWith("status-connector-")) {
                            if (!statusesMap.get(connectCluster).containsKey(key.substring(17))) {
                                statusesMap.get(connectCluster).put(key.substring(17), new HashMap<String,String>());
                            }
                            statusesMap.get(connectCluster).get(key.substring(17)).put("connector", value);
                        } else if (value != null && !value.contains("UNASSIGNED") && key.startsWith("status-task-")) {
                            String connectorName = key.substring(12).replaceAll("-[0-9]*$", "");
                            if (!statusesMap.get(connectCluster).containsKey(connectorName)) {
                                statusesMap.get(connectCluster).put(connectorName, new HashMap<String,String>());
                            }
                            statusesMap.get(connectCluster).get(connectorName).put(key.substring(12).replace(connectorName, "task"), value);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            // start Kafka Streams
            @SuppressWarnings("resource")
            final KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.cleanUp();
            streams.start();
        }
    }

    //temp
    @GET
    @Path("/db")
    public Response getDb() throws IOException {
        return Response.ok(clusterToServerToDatabaseToConnector).build();
    }

    @GET
    @Path("/configs")
    public Response getAllConfigs() throws IOException {
        return Response.ok(configsMap).build();
    }
    
    @GET
    public Response getClusters() {
        return Response.ok(connectClusters).build();
    }

    @GET
    @Path("/{cluster}")
    public Response getClusterInfo(@PathParam("cluster") String cluster) throws IOException {
        if (!(prop.getProperty(cluster + ".offset") == null) && !(prop.getProperty(cluster + ".config") == null) && !(prop.getProperty(cluster + ".bootstrap_servers") == null)) {
            ObjectMapper sortedMapper = new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
            return Response.ok(
                sortedMapper.createObjectNode()
                    .put("bootstrap.servers", prop.getProperty(cluster + ".bootstrap_servers"))
                    .put("offset", prop.getProperty(cluster + ".offset"))
                    .put("config", prop.getProperty(cluster + ".config"))
                    .put("status", prop.getProperty(cluster + ".status"))
                ).build();
        } else {
            return Response.ok("[" + cluster + "] is not a valid cluster.").build();
        }
    }

    @GET
    @Path("/statuses")
    public Response getStatuses() throws IOException {
        return Response.ok(statusesMap).build();
    }

    @GET
    @Path("/statuses/{cluster}")
    public Response getStatusesCluster(@PathParam("cluster") String cluster) throws IOException {
        return Response.ok(statusesMap.get(cluster)).build();
    }

    @GET
    @Path("/{cluster}/status")
    public Response getClusterStatus(@PathParam("cluster") String cluster) throws IOException {
        try {
            return Response.ok(statusesMap.get(cluster).entrySet().stream().filter(e -> {
                return e.getValue() != null;
            }).collect(Collectors.toMap(e-> e.getKey(), e-> e.getValue()))).build();
        } catch (NullPointerException npe) {
            return Response.ok("Error: [" + cluster + "] is not a valid Connect cluster.").build();
        }
    }

    @GET
    @Path("/statuses/failed")
    public Response getAllStatusFailed(@PathParam("cluster") String cluster, @PathParam("status") String status)
            throws IOException {
        HashMap<String, HashMap<String, HashMap<String, String>>> outerHashmap = new HashMap<String, HashMap<String, HashMap<String, String>>>();
        statusesMap.entrySet().stream().forEach(n -> {
            HashMap<String, HashMap<String, String>> innerHashmap = new HashMap<String, HashMap<String, String>>();
            innerHashmap.putAll(n.getValue().entrySet().stream().filter(e -> {
                return e.getValue() != null && e.getValue().toString().contains("FAILED");
            }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
            if (!innerHashmap.isEmpty()) {
                outerHashmap.put(n.getKey(), innerHashmap);
            }
        });
        return Response.ok(outerHashmap).build();
    }

    @GET
    @Path("/{cluster}/status/failed")
    public Response getClusterStatusFailed(@PathParam("cluster") String cluster, @PathParam("status") String status) throws IOException {
        try {
            return Response.ok(statusesMap.get(cluster).entrySet().stream().filter(e -> {
                return e.getValue() != null && e.getValue().toString().contains("FAILED");
            }).collect(Collectors.toMap(e-> e.getKey(), e-> e.getValue()))).build();
        } catch (NullPointerException npe) {
            return Response.ok("Error: [" + cluster + "] is not a valid Connect cluster.").build();
        }
    }
    

    @GET
    @Path("/{cluster}/connectors")
    public Response getConnectors(@PathParam("cluster") String cluster) throws IOException {
        try {
            Stream<Object> connectors = configsMap.get(cluster).entrySet().stream().filter(e -> e.getKey().startsWith("connector-") && e.getValue() != null)
                    .map(e -> e.getKey().substring(10));
            return Response.ok(connectors).build();
        } catch (NullPointerException npe) {
            return Response.ok("Error: [" + cluster + "] is not a valid Connect cluster.").build();
        }
    }

    @GET
    @Path("/{cluster}/connectors/{connector}")
    public Response getConnector(@PathParam("cluster") String cluster, @PathParam("connector") String connector)
            throws IOException {
        String properties = configsMap.get(cluster).get("connector-" + connector);
        if (properties == null) {
            return Response
                    .ok("Error: Connector [" + connector + "] does not exist on Connect cluster [" + cluster + "].")
                    .build();
        }
        return Response.ok(properties).build();
    }

    @GET
    @Path("/{cluster}/connectors/{connector}/config")
    public Response getConnectorConfig(@PathParam("cluster") String cluster, @PathParam("connector") String connector)
            throws IOException {
        String properties = configsMap.get(cluster).get("connector-" + connector);
        if (properties == null) {
            return Response
                    .ok("Error: Connector [" + connector + "] does not exist on Connect cluster [" + cluster + "].")
                    .build();
        }
        return Response.ok(properties).build();
    }

    @GET
    @Path("/{cluster}/offsets")
    public Response getOffsets(@PathParam("cluster") String cluster, @PathParam("connector") String connector)
            throws IOException {
        return Response
                .ok(offsetsMap.get(cluster))
                .build();
    }

    @GET
    @Path("/{cluster}/offsets/{connector}")
    public Response getOffsetsConnector(@PathParam("cluster") String cluster, @PathParam("connector") String connector)
            throws IOException {
        return Response.ok(offsetsMap.get(cluster).entrySet().stream()
                .filter(s -> s.getKey().matches("\\[\"" + connector + "\",.*\\]"))
                .collect(Collectors.toMap(e -> e.getKey().substring(e.getKey().indexOf(",") + 1), e -> e.getValue())))
                .build();
    }

    @GET
    @Path("/{cluster}/offsets/{connector}/{offsetKey}")
    public Response getOffsetKey(@PathParam("cluster") String cluster, @PathParam("connector") String connector,
            @PathParam("offsetKey") String offsetKey) throws IOException {
        return Response.ok(offsetsMap.get(cluster).entrySet().stream()
                .filter(s -> s.getKey().matches("\\[\"" + connector + "\",.*\\]") && s.getKey().contains(offsetKey))
                .collect(Collectors.toMap(e -> e.getKey().substring(e.getKey().indexOf(",") + 1), e -> e.getValue())))
                .build();
    }

    @PUT
    @Path("/{cluster}/offsets/{connector}/{offsetKey}/{offset}")
    public Response setOffset(@PathParam("cluster") String cluster, @PathParam("connector") String connector,
            @PathParam("offsetKey") String offsetKey, @PathParam("offset") long offset) throws IOException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty(cluster + ".bootstrap_servers"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "meld-" + System.currentTimeMillis());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        offsetsMap.get(cluster).entrySet().stream()
                .filter(s -> s.getKey().matches("\\[\"" + connector + "\",.*\\]") && s.getKey().contains(offsetKey)).forEach(e -> {
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
        sendPostRequest(prop.getProperty(cluster + ".url") + "/connectors/" + connector + "/tasks/0/restart", "");
        return Response.ok().build();

    }

    @GET
    @Path("/{cluster}/export")
    public Response getExport(@PathParam("cluster") String cluster) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);
        for (Entry<String, String> e : configsMap.get(cluster).entrySet()) {
            if (e.getKey().startsWith("connector-") && e.getValue() != null) {
                ZipEntry entry = new ZipEntry(e.getKey().substring(10) + ".json");
                zos.putNextEntry(entry);
                ObjectMapper sortedMapper = new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY,
                        true);
                JsonNode json = new ObjectMapper().readTree(e.getValue().substring(14, e.getValue().length() - 1));
                ObjectNode obj = (ObjectNode) json;
                obj.remove("name");
                zos.write(sortedMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(obj));
                zos.closeEntry();
            }
        }
        zos.close();
        baos.close();

        return Response.ok(baos.toByteArray()).type("application/zip")
                .header("Content-Disposition", "attachment; filename=\"" + cluster + ".zip\"").build();
    }

    private Response sendPutRequest(String url, String body) throws IOException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        return invocationBuilder.put(Entity.entity(body, MediaType.APPLICATION_JSON));
    }
    
    private Response sendPostRequest(String url, String body) throws IOException {
        Client client = ClientBuilder.newClient();
        WebTarget webTarget = client.target(url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        return invocationBuilder.post(Entity.entity(body, MediaType.APPLICATION_JSON));
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