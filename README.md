# simple-kafka

A simple Kafka client written in Java that demonstrates the minimal configuration needed to
produce/consume events to/from a Kafka Broker.

## Kafka Broker

This project uses the [confluentinc/cp-kafka](https://hub.docker.com/r/confluentinc/cp-kafka) docker image as defined in the [docker-compose.yml](https://github.com/alexandrebaldo/simple-kafka/blob/main/docker-compose.yml) file.

## The Producer

A simple producer that reads a text file and produces an event to the Kafka Broker for each line of the file.
The lines of the file are formatted as **key|value** and the client tries to parse the lines before publishing the events.

## The consumer

A very basic consumer client that runs an infinity loop calling KafkaConsumer#poll(...). It prints out the Topic, Partition, Index, Key, and Value for each event received from the broker.

# The Application

**Note: You need to have Docker installed and running before executing the next steps.**

### Setting up the environment

In order to run the application we need to first start the Kafka environment and then create a new topic.

In the commands bellow, **broker** is the name of the container running Kafka. This is defined in the [docker-compose.yml](https://github.com/alexandrebaldo/simple-kafka/blob/main/docker-compose.yml) file.

We are creating a new topic named **books** with 6 partitions.

1. Start the Kafka environment. In the root of you project type:

   ```docker compose up -d```

2. Login into the container:

   ```docker exec -it broker bash```

3. Create the Kafka topic:

   ```kafka-topics --create --topic books --partitions 6 --bootstrap-server broker:9092```

4. Type ```exit``` to exit the container.

## Running the Application

You can run the app directly in your IDE. Both classes **SimpleProducer** and **SimpleConsumer** contain a main method.

Start the consumer first, and it will run indefinitely waiting for new events.

Then start the producer, and it will read the **events.txt** file located in the **src/main/resources** folder and publish the events to the broker.
The producer stops after the last message is published.
Every time you start the producer the same messages will be posted unless you modify the **events.txt** file.
