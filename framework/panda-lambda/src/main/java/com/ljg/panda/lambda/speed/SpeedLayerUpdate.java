package com.ljg.panda.lambda.speed;

import com.ljg.panda.api.TopicProducer;
import com.ljg.panda.api.speed.SpeedModelManager;
import com.ljg.panda.lambda.TopicProducerImpl;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Main Spark Streaming function for the speed layer that collects and publishes update to
 * a Kafka topic.
 *  接受新的原始数据，更新内存中的模型，然后将模型中更新的数据发送到updateTopic中，供给Serving Layer消费并更新模型
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
final class SpeedLayerUpdate<K, M, U> implements VoidFunction<JavaPairRDD<K, M>> {

    private static final Logger log = LoggerFactory.getLogger(SpeedLayerUpdate.class);

    private final SpeedModelManager<K, M, U> modelManager;
    private final String updateBroker;
    private final String updateTopic;

    SpeedLayerUpdate(SpeedModelManager<K, M, U> modelManager, String updateBroker, String updateTopic) {
        this.modelManager = modelManager;
        this.updateBroker = updateBroker;
        this.updateTopic = updateTopic;
    }

    @Override
    public void call(JavaPairRDD<K, M> newData) throws IOException {
        if (newData.isEmpty()) {
            log.debug("RDD was empty");
        } else {
            // 更新Model，且返回Model中更新了的数据
            Iterable<U> updates = modelManager.buildUpdates(newData);
            // 如果更新的数据不为空的话，则发往updateTopic
            if (updates != null) {
                try (TopicProducer<String, U> producer = new TopicProducerImpl<>(updateBroker, updateTopic, true)) {
                    updates.forEach(update -> producer.send("UP", update));
                }
            }
        }
    }

}
