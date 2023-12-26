package com.devspace.simplekafka.producer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SimpleProducerTest {

  @Test
  @DisplayName("Should create producer records")
  void shouldCreateProducerRecords(@Mock KafkaProducer<String, String> kafkaProducer)
      throws URISyntaxException, IOException {
    SimpleProducer simpleProducer = new SimpleProducer(kafkaProducer);

    List<ProducerRecord<String, String>> producerRecords =
        simpleProducer.buildProducerRecords("/events.txt");

    assertAll(
        () -> Assertions.assertThat(producerRecords).hasSize(2),
        () ->
            Assertions.assertThat(Objects.requireNonNull(producerRecords).getFirst().key())
                .isEqualTo("1"),
        () ->
            Assertions.assertThat(Objects.requireNonNull(producerRecords).getFirst().value())
                .isEqualTo("book 1"),
        () ->
            Assertions.assertThat(Objects.requireNonNull(producerRecords).get(1).key())
                .isNull(),
        () ->
            Assertions.assertThat(Objects.requireNonNull(producerRecords).get(1).value())
                .isEqualTo("book 2"));
  }

  @Test
  @DisplayName("Should produce event")
  void shouldProduceEvent(@Mock KafkaProducer<String, String> kafkaProducer)
      throws URISyntaxException, IOException {
    SimpleProducer simpleProducer = new SimpleProducer(kafkaProducer);

    List<ProducerRecord<String, String>> producerRecords =
        simpleProducer.buildProducerRecords("/events.txt");

    simpleProducer.produce(producerRecords.getFirst());

    Mockito.verify(kafkaProducer)
        .send(Mockito.eq(producerRecords.getFirst()), Mockito.any(Callback.class));
  }
}
