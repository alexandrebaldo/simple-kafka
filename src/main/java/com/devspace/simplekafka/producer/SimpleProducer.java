package com.devspace.simplekafka.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleProducer {

  public static final String TOPIC_NAME = "books";
  private final KafkaProducer<String, String> kafkaProducer;

  // -------------------- Constructors

  public SimpleProducer(KafkaProducer<String, String> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  // -------------------- Class methods

  public static void main(String[] args) {
    final Map<String, Object> config = buildProducerConfig();

    final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(config);

    final SimpleProducer simpleProducer = new SimpleProducer(kafkaProducer);

    try {
      simpleProducer.buildProducerRecords("/events.txt").forEach(simpleProducer::produce);

      simpleProducer.shutdown();
    } catch (Exception ex) {
      System.out.println("Exception occurred: " + ex);
    }
  }

  private static Map<String, Object> buildProducerConfig() {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
  }

  private static ProducerRecord<String, String> parseEvent(String event) {

    String key = null;
    String value;

    // We are expecting events to be in the format key|value
    // If it's not in that format then we use the entire String as the value
    final String[] tokens = event.split("\\|");

    if (tokens.length > 1) {
      key = tokens[0];
      value = tokens[1];
    } else {
      value = tokens[0];
    }

    return new ProducerRecord<>(TOPIC_NAME, key, value);
  }

  private static void printMetadata(RecordMetadata metadata) {
    System.out.println(
        "Topic: "
            + metadata.topic()
            + ", Partition: "
            + metadata.partition()
            + ", Offset: "
            + metadata.offset());
  }

  // -------------------- Instance methods

  public List<ProducerRecord<String, String>> buildProducerRecords(final String file)
      throws URISyntaxException, IOException {
    final URL url = Objects.requireNonNull(getClass().getResource(file));

    try (Stream<String> lines = Files.lines(Paths.get(url.toURI()))) {
      return lines.map(SimpleProducer::parseEvent).toList();
    }
  }

  public void produce(ProducerRecord<String, String> producerRecord) {
    kafkaProducer.send(producerRecord, (metadata, exception) -> printMetadata(metadata));
  }

  public void shutdown() {
    kafkaProducer.close();
  }
}
