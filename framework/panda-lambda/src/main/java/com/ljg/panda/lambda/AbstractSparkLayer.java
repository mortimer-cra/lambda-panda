package com.ljg.panda.lambda;

import com.google.common.base.Preconditions;
import com.ljg.panda.common.lang.ClassUtils;
import com.ljg.panda.common.settings.ConfigUtils;
import com.ljg.panda.kafka.util.KafkaUtils;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates commonality between Spark-based layer processes,
 *
 * 基于Spark Streaming的通用处理流程的抽象封装
 * 因为我们的Batch Layer和Speed Layer都是基于Spark Streaming实现的，所以我们可以将一些通用的流程封装在这个抽象类中
 * 比如：初始化StreamingContext和创建Kafka的direct模型的InputStreaming
 * 因为这些流程不管是Batch Layer还是Speed Layer都会用的到
 *
 * {@link com.ljg.panda.lambda.batch.BatchLayer} and
 * {@link com.ljg.panda.lambda.speed.SpeedLayer}
 *
 * @param <K> input topic key type
 * @param <M> input topic message type
 */
public abstract class AbstractSparkLayer<K, M> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractSparkLayer.class);

    private final Config config; // 配置信息
    private final String id; // 唯一标识
    private final String streamingMaster; // 指定Spark应用的--master
    private final String inputTopic; // 输入原始数据的topic
    private final String inputTopicLockMaster; // inputTopic 对应的 zkServer
    private final String inputBroker; // Kafka的broker server
    private final String updateTopic; // model更新的topic
    private final String updateTopicLockMaster; // updateTopic 对应的 zkServer
    private final Class<K> keyClass; // Kafka中消息的key的Class
    private final Class<M> messageClass; // Kafka中消息的value的Class
    private final Class<? extends Deserializer<K>> keyDecoderClass; // Kafka中消息的key的解码器的Class
    private final Class<? extends Deserializer<M>> messageDecoderClass; // Kafka中消息的value的解码器的Class
    private final int generationIntervalSec; // Spark Streaming应用程序的batch time
    private final Map<String, Object> extraSparkConfig; // 其他的Spark应用的配置

    /**
     *  构造器
     * @param config 需要的配置对象
     */
    protected AbstractSparkLayer(Config config) {
        Objects.requireNonNull(config);
        log.info("Configuration:\n{}", ConfigUtils.prettyPrint(config));

        String group = getConfigGroup();
        this.config = config;
        String configuredID = ConfigUtils.getOptionalString(config, "panda.id");
        this.id = configuredID == null ? UUID.randomUUID().toString() : configuredID;
        this.streamingMaster = config.getString("panda." + group + ".streaming.master");
        this.inputTopic = config.getString("panda.input-topic.message.topic");
        this.inputTopicLockMaster = config.getString("panda.input-topic.lock.master");
        this.inputBroker = config.getString("panda.input-topic.broker");
        this.updateTopic = ConfigUtils.getOptionalString(config, "panda.update-topic.message.topic");
        this.updateTopicLockMaster = ConfigUtils.getOptionalString(config, "panda.update-topic.lock.master");
        this.keyClass = ClassUtils.loadClass(config.getString("panda.input-topic.message.key-class"));
        this.messageClass =
                ClassUtils.loadClass(config.getString("panda.input-topic.message.message-class"));
        this.keyDecoderClass = (Class<? extends Deserializer<K>>) ClassUtils.loadClass(
                config.getString("panda.input-topic.message.key-decoder-class"), Deserializer.class);
        this.messageDecoderClass = (Class<? extends Deserializer<M>>) ClassUtils.loadClass(
                config.getString("panda.input-topic.message.message-decoder-class"), Deserializer.class);
        this.generationIntervalSec = config.getInt("panda." + group + ".streaming.generation-interval-sec");

        this.extraSparkConfig = new HashMap<>();
        config.getConfig("panda." + group + ".streaming.config").entrySet().forEach(e ->
                extraSparkConfig.put(e.getKey(), e.getValue().unwrapped())
        );

        Preconditions.checkArgument(generationIntervalSec > 0);
    }

    /**
     * @return layer-specific config grouping under "panda", like "batch" or "speed"
     */
    protected abstract String getConfigGroup();

    /**
     * @return display name for layer like "BatchLayer"
     */
    protected abstract String getLayerName();

    protected final Config getConfig() {
        return config;
    }

    protected final String getID() {
        return id;
    }

    protected final String getGroupID() {
        return "pandaGroup-" + getLayerName() + "-" + getID();
    }

    protected final String getInputTopicLockMaster() {
        return inputTopicLockMaster;
    }

    protected final Class<K> getKeyClass() {
        return keyClass;
    }

    protected final Class<M> getMessageClass() {
        return messageClass;
    }

    /**
     *  初始化Spark Streaming的StreamingContext
     * @return StreamingContext
     */
    protected final JavaStreamingContext buildStreamingContext() {
        log.info("Starting SparkContext with interval {} seconds", generationIntervalSec);

        SparkConf sparkConf = new SparkConf();

        // Only for tests, really
        if (sparkConf.getOption("spark.master").isEmpty()) {
            log.info("Overriding master to {} for tests", streamingMaster);
            sparkConf.setMaster(streamingMaster);
        }
        // Only for tests, really
        if (sparkConf.getOption("spark.app.name").isEmpty()) {
            String appName = "panda" + getLayerName();
            if (id != null) {
                appName = appName + "-" + id;
            }
            log.info("Overriding app name to {} for tests", appName);
            sparkConf.setAppName(appName);
        }
        extraSparkConfig.forEach((key, value) -> sparkConf.setIfMissing(key, value.toString()));

        // Turn this down to prevent long blocking at shutdown
        sparkConf.setIfMissing(
                "spark.streaming.gracefulStopTimeout",
                Long.toString(TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS)));
        sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));
        long generationIntervalMS =
                TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
        return new JavaStreamingContext(jsc, new Duration(generationIntervalMS));
    }

    /**
     * 初始化InputDStream
     * @param streamingContext
     * @return 初始化好的InputDStream
     */
    protected final JavaInputDStream<ConsumerRecord<K, M>> buildInputDStream(
            JavaStreamingContext streamingContext) {

        Preconditions.checkArgument(
                KafkaUtils.topicExists(inputTopicLockMaster, inputTopic),
                "Topic %s does not exist; did you create it?", inputTopic);
        if (updateTopic != null && updateTopicLockMaster != null) {
            Preconditions.checkArgument(
                    KafkaUtils.topicExists(updateTopicLockMaster, updateTopic),
                    "Topic %s does not exist; did you create it?", updateTopic);
        }

        String groupID = getGroupID();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("group.id", groupID);
        // Don't re-consume old messages from input by default
        kafkaParams.put("auto.offset.reset", "latest"); // Ignored by Kafka 0.10 Spark integration
        kafkaParams.put("bootstrap.servers", inputBroker);
        kafkaParams.put("key.deserializer", keyDecoderClass.getName());
        kafkaParams.put("value.deserializer", messageDecoderClass.getName());

        LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
        ConsumerStrategy<K, M> consumerStrategy = ConsumerStrategies.Subscribe(
                Collections.singleton(inputTopic), kafkaParams, Collections.emptyMap());
        // 创建Kafka Direct方式的InputDStream
        return org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
                streamingContext,
                locationStrategy,
                consumerStrategy);
    }

}
