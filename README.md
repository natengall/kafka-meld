# kafka-meld
Meld is a REST API that integrates with Kafka and Kafka Connect clusters in order to simplify administrative operations in the event streaming space. It achieves this by communicating directly with each Kafka Connect cluster's internal topics to observe the cluster's state and to apply changes to them that are otherwise not easily accessible. The Meld API aims to be a community-driven project to add to the overall tooling that's available for administering Kafka and Kafka Connect.

Some of the tasks that Meld helps assist with today include:
- managing Kafka Connect source connector offsets
- exporting all connector properties from a Kafka Connect cluster

## Getting started
Meld contains two configuration files that users will need to configure before starting up the application. They are `kafka.properties` and `connect.properties`.

The `kafka.properties` file is used to list all of the Kafka clusters that Meld will integrate with.
```
kafkac1.bootstrap_servers=<kafkac1-node1>:9092,<kafkac1-node2>:9092,<kafkac1-node3>:9092
kafkac2.bootstrap_servers=<kafkac2-node1>:9092,<kafkac2-node2>:9092,<kafkac2-node3>:9092
```

The `connect.properties` file is used to list all of the Kafka Connect clusters that Meld will integrate with.
```
connectc1.bootstrap_servers=<kafkac1-node1>:9092
connectc1.url=<connectc1-node1>:8083
connectc1.offset=<connectc1's offset.storage.topic>
connectc1.config=<connectc1's config.storage.topic>
````

Once these two configuration files are defined specific to your environment, we are ready to start up the application! 

```
mvn clean install
java -jar target/Meld-0.1-SNAPSHOT.jar server config.yml
```

Check to see that the application is running by sending a GET request to `localhost:9000`. The root path of the Meld service will display the services you can manage through its REST API.

```
$ curl localhost:9000
    ["connect","kafka"]
```

## Meld API - Connect

To explore the Kafka Connect clusters integrated with Meld, simply append `/connect` to the root path.

```
$ curl localhost:9000/connect
    ["connectc1","connectc2","connectc3"]
```

Appending a Kafka Connect cluster name here will present you with a list of all connectors on that cluster.
```
$ curl localhost:9000/connect/connectc1
    ["connector1","connector2","connector3"]
```

Further appending the connector name brings up the current configurations for that connector, based on an internal hash table being updated at runtime by stream processing the Connect cluster's `config.storage.topic`.

```
$ curl localhost:9000/connect/connectc1/connector1
    {"properties":{"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector", ... }}
```

### Managing Kafka Connect source connector offsets
As of Kafka 2.3, Kafka Connect does not provide an API for managing offsets. Source connector offsets are stored within the Kafka Connect cluster's `offset.storage.topic`, which means that it is possible to bring up 1.) consumer instances to gain visibility into source connector offsets and 2.) producer instances to plant offsets for source connectors to rewind to or start from.

**Gaining visiblity into source connector offsets**
```
$ curl localhost:9000/connect/connectc1/connector1/offset
    [{"{\"table\":\"table1\"}":"{\"incrementing\":684046074}","{\"table\":\"table2\"}":"{\"incrementing\":684044041}","{\"table\":\"table3\"}":"{\"incrementing\":684046016}"}]

$ curl localhost:9000/connect/connectc1/connector1/offset/table1
    {"{\"table\":\"table1\"}":"{\"incrementing\":684046074}"}
```

**Planting source connector offsets**
```
$ curl --request PUT localhost:9000/connect/connectc1/connector1/offset/table1/684046000
```
This request effectively brings up a producer client to write to the internal offset topic used by the connectc1 cluster and subsequently triggering a restart of that task to force that offset to apply.

### Exporting connector properties

Appending `/export` to the cluster name in the URL results in a zip file with all of the connectors currently on that Kafka Connect cluster.

```
$ curl localhost:9000/connect/connectc1/export > connectc1.zip
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 11844    0 11844    0     0   246k      0 --:--:-- --:--:-- --:--:--  373k

$ unzip connectc1.zip -d connectc1
Archive:  connectc1.zip
  inflating: connector1.json
  inflating: connector2.json
  inflating: connector3.json

$ cat connectc1/connector1.json
{
  "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector",
  ...
}
```