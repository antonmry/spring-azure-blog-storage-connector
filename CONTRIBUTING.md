
## Integration tests

Start kafka:

```
cd env
docker-compose up -d
```

Create the topic for testing:

```
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testEmbeddedOut
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testEmbeddedOut2
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dead-out
kafka-topics --list --bootstrap-server localhost:9092
```

Monitor DLQ:

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic dead-out
```

Launch the test:

```
mvn verify -Pfailsafe
```
