package com.github.meld;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
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
import org.apache.kafka.common.TopicPartition;

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