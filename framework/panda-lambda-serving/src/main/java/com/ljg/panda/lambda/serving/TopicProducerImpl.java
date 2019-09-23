package com.ljg.panda.lambda.serving;

import com.ljg.panda.api.TopicProducer;
import com.ljg.panda.common.settings.ConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Wraps access to a Kafka message topic {@link Producer}.
 *
 * @param <K> key type to send
 * @param <M> message type to send
 */
public final class TopicProducerImpl<K,M> implements TopicProducer<K,M> {

  private final String updateBroker;
  private final String topic;
  private Producer<K,M> producer;

  public TopicProducerImpl(String updateBroker, String topic) {
    this.updateBroker = updateBroker;
    this.topic = topic;
  }

  @Override
  public String getUpdateBroker() {
    return updateBroker;
  }

  @Override
  public String getTopic() {
    return topic;
  }

  private synchronized Producer<K,M> getProducer() {
    // Lazy init
    if (producer == null) {
      producer = new KafkaProducer<>(ConfigUtils.keyValueToProperties(
          "bootstrap.servers", updateBroker,
          "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
          "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
          "linger.ms", 1000, // Make configurable?
          "compression.type", "gzip",
          "acks", 1,
          "max.request.size", 1 << 26 // TODO
      ));
    }
    return producer;
  }

  @Override
  public void send(K key, M message) {
    getProducer().send(new ProducerRecord<>(topic, key, message));
  }

  @Override
  public synchronized void close() {
    if (producer != null) {
      producer.close();
    }
  }

}
