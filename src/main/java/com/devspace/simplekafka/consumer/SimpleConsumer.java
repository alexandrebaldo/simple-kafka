package com.devspace.simplekafka.consumer;

import static com.devspace.simplekafka.producer.SimpleProducer.TOPIC_NAME;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {

  private final KafkaConsumer<String, String> kafkaConsumer;

  // -------------------- Constructors

  public SimpleConsumer(KafkaConsumer<String, String> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  // -------------------- Class methods

  public static void main(String[] args) {
    final Map<String, Object> config = getConsumerConfig();

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config)) {
      kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));

      final SimpleConsumer simpleConsumer = new SimpleConsumer(kafkaConsumer);

      while (true) {
        simpleConsumer.consume();
      }
    }
  }

  private static void printRecord(ConsumerRecord<String, String> consumerRecord) {
    System.out.printf(
        "Topic: %s, Partition: %s, Offset: %s, Key: %s, Value: %s%n",
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.key(),
        consumerRecord.value());
  }

  private static Map<String, Object> getConsumerConfig() {
    return Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:29092",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest",
        ConsumerConfig.GROUP_ID_CONFIG,
        "simple_kafka_consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
  }

  // -------------------- Instance methods

  public void consume() {
    final ConsumerRecords<String, String> consumerRecords =
        kafkaConsumer.poll(Duration.ofSeconds(1));

    consumerRecords.forEach(SimpleConsumer::printRecord);
  }
}
