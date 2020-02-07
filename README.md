
## Setup and test

Start kafka:

```
cd env
docker-compose up -d
```

Create the topic for testing:

```
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testEmbeddedOut
kafka-topics --list --bootstrap-server localhost:9092
```


