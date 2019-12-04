package com.github.meld;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.Collator;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("kafka")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaController {

    Properties prop = new Properties();
    LinkedList<String> kafkaClusters = new LinkedList<String>();
    String[] kafkaClusterActions = { "consumers", "topics", "clusterId" };
    String[] kafkaTopicActions = { "purge", "delete" };

    HashMap<String, AdminClient> adminClients = new HashMap<>();

    public KafkaController() throws FileNotFoundException, IOException {
        prop.load(new FileInputStream("kafka.properties"));
        prop.keySet().stream().forEach(key -> {
            String keyString = key.toString();
            if (keyString.endsWith(".bootstrap_servers")) {
                String clusterName = keyString.replaceAll(".bootstrap_servers", "");
                kafkaClusters.add(clusterName);
            }
        });
        kafkaClusters.sort(new Comparator<String>() {
            public int compare(String o1, String o2) {
                return Collator.getInstance().compare(o1, o2);
            }
        });
        for (String kafkaCluster : kafkaClusters) {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    prop.getProperty(kafkaCluster + ".bootstrap_servers"));
            AdminClient adminClient = AdminClient.create(config);
            adminClients.put(kafkaCluster, adminClient);
        }
    }

    @GET
    public Response getKafkaClusters() throws IOException {
        return Response.ok(kafkaClusters).build();
    }

    @GET
    @Path("/{cluster}")
    public Response getKafkaClusterActions() throws IOException {
        return Response.ok(kafkaClusterActions).build();
    }

    @GET
    @Path("/{cluster}/consumers")
    public Response getConsumerGroups(@PathParam("cluster") String cluster)
            throws InterruptedException, ExecutionException {
        LinkedList<String> consumerGroupList = new LinkedList<String>();
        for (ConsumerGroupListing listing : adminClients.get(cluster).listConsumerGroups().all().get()) {
            consumerGroupList.add(listing.groupId());
        }
        return Response.ok(consumerGroupList).build();
    }

    @GET
    @Path("/{cluster}/consumers/{group}")
    public Response getConsumerGroupOffsets(@PathParam("cluster") String cluster, @PathParam("group") String group)
            throws InterruptedException, ExecutionException, TimeoutException {
        ObjectMapper sortedMapper = new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        return Response.ok(adminClients.get(cluster).listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS).entrySet().stream()
                .map(e -> sortedMapper.createObjectNode().put(e.getKey().toString(), e.getValue().offset()))
                ).build();
    }

    @GET
    @Path("/{cluster}/consumersz/{group}")
    public Response getPrefixConsumerGroupOffsets(@PathParam("cluster") String cluster, @PathParam("group") String group)
            throws InterruptedException, ExecutionException, TimeoutException {
        HashMap<String, HashMap<String, Long>> offsetsz = new HashMap<String, HashMap<String, Long>>(); 
        List<String> matchingConsumerGroups = adminClients.get(cluster).listConsumerGroups().all().get().stream().filter(x -> {return x.groupId().startsWith(group);}).map(y -> {return y.groupId();}).collect(Collectors.toList());
        for(String matchingConsumerGroup : matchingConsumerGroups) {
            offsetsz.put(matchingConsumerGroup, new HashMap<String, Long>());
            
            for(Entry<TopicPartition,OffsetAndMetadata> entry : adminClients.get(cluster).listConsumerGroupOffsets(matchingConsumerGroup).partitionsToOffsetAndMetadata().get().entrySet()) {
                offsetsz.get(matchingConsumerGroup).put(entry.getKey().toString(), entry.getValue().offset());
            }
        }
        return Response.ok(offsetsz).build();
    }

    @DELETE
    @Path("/{cluster}/consumersz/{group}")
    public Response deleteConsumerGroupOffsets(@PathParam("cluster") String cluster, @PathParam("group") String group)
            throws InterruptedException, ExecutionException, TimeoutException {
        
        List<String> a = adminClients.get(cluster).listConsumerGroups().all().get().stream().filter(x -> {return x.groupId().startsWith(group);}).map(y -> {return y.groupId();}).collect(Collectors.toList());
        HashMap<String, List<String>> hm = new HashMap<String,  List<String>>();
        adminClients.get(cluster).deleteConsumerGroups(a);
        return Response.ok(a).build();
    }

    @GET
    @Path("/{cluster}/consumers/{group}/{topic}")
    public Response getConsumerGroupOffsetsForTopic(@PathParam("cluster") String cluster, @PathParam("group") String group, @PathParam("topic") String topic)
            throws InterruptedException, ExecutionException, TimeoutException {
        ObjectMapper sortedMapper = new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        return Response.ok(adminClients.get(cluster).listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS).entrySet().stream().filter(e -> {return e.getKey().toString().startsWith(topic);})
                .map(e -> sortedMapper.createObjectNode().put(e.getKey().toString(), e.getValue().offset()))
                ).build();
    }

    @GET
    @Path("/{cluster}/consumers/{group}/{topic}/{partition}")
    public Response getConsumerGroupOffsetsForTopicPartition(@PathParam("cluster") String cluster, @PathParam("group") String group, @PathParam("topic") String topic, @PathParam("partition") String partition)
            throws InterruptedException, ExecutionException, TimeoutException {
        ObjectMapper sortedMapper = new ObjectMapper().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        return Response.ok(adminClients.get(cluster).listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS).entrySet().stream().filter(e -> {return e.getKey().toString().startsWith(topic) && e.getKey().partition() == Integer.parseInt(partition);})
                .map(e -> sortedMapper.createObjectNode().put(e.getKey().toString(), e.getValue().offset()))
                ).build();
    }

    @DELETE
    @Path("/{cluster}/consumers/{group}")
    public Response deleteConsumerGroupOffsetsTopicPartition(@PathParam("cluster") String cluster, @PathParam("group") String group) {
        return Response.ok(
            adminClients.get(cluster).deleteConsumerGroups(Collections.singletonList(group))
        ).build();
    }

    @PUT
    @Path("/{cluster}/consumers/{group}/{topic}/{partition}/{offset}")
    public Response updateConsumerGroupOffsetsTopicPartition(@PathParam("cluster") String cluster, @PathParam("group") String group, 
            @PathParam("topic") String topic, @PathParam("partition") String partition, @PathParam("offset") String offset) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.get(cluster + ".bootstrap_servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.assign(Collections.singletonList(new TopicPartition(topic, Integer.parseInt(partition))));
        consumer.seek(new TopicPartition(topic, Integer.parseInt(partition)), Long.parseLong(offset));
        try {
            consumer.commitSync();
            consumer.close();
            return Response.ok("[" + group + "] is now at offset [" + offset + "]" + " for [" + topic + "-" + partition + "]").build();
        } catch (CommitFailedException cfe) {
            consumer.close();
            return Response.ok("Could not reset offset because another consumer client is still reading from this topic partition.").build();
        }
    }

    @GET
    @Path("/{cluster}/clusterId")
    public Response getClusterId(@PathParam("cluster") String cluster) throws InterruptedException, ExecutionException {
        return Response.ok(adminClients.get(cluster).describeCluster().clusterId().get()).build();
    }

    @GET
    @Path("/{cluster}/topics")
    public Response getTopics(@PathParam("cluster") String cluster)
            throws IOException, InterruptedException, ExecutionException {
        return Response.ok(adminClients.get(cluster).listTopics().names().get()).build();
    }

    @GET
    @Path("/{cluster}/topics/{topic}")
    public Response getTopicActions(@PathParam("cluster") String cluster, @PathParam("topic") String topic)
            throws IOException, InterruptedException, ExecutionException {
        return Response.ok(kafkaTopicActions).build();
    }

    @SuppressWarnings("serial")
    @GET
    @Path("/{cluster}/topics/{topic}/purge")
    public Response purge(@PathParam("cluster") String cluster, @PathParam("topic") String topic)
            throws IOException, InterruptedException, ExecutionException {
        return Response.ok(adminClients.get(cluster).deleteRecords(new HashMap<TopicPartition, RecordsToDelete>() {
            {
                put(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(Long.MAX_VALUE));
            }
        }).all()).build();
    }

    @GET
    @Path("/{cluster}/topics/{topic}/delete")
    public Response delete(@PathParam("cluster") String cluster, @PathParam("topic") String topic)
            throws IOException, InterruptedException, ExecutionException {
        return Response.ok(adminClients.get(cluster).deleteTopics(Arrays.asList(topic)).all()).build();
    }

    @GET
    @Path("/{cluster}/createTopic/{topic}")
    public Response createTopic(@PathParam("cluster") String cluster, @PathParam("topic") String topic)
            throws IOException, InterruptedException, ExecutionException {
        return Response.ok(adminClients.get(cluster).createTopics(Arrays.asList(new NewTopic(topic, 1, (short) 3)),
                new CreateTopicsOptions())).build();
    }
}