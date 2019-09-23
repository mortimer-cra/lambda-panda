package com.ljg.panda.lambda.speed;

import com.google.common.base.Preconditions;
import com.ljg.panda.api.KeyMessage;
import com.ljg.panda.api.speed.SpeedModelManager;
import com.ljg.panda.common.collection.CloseableIterator;
import com.ljg.panda.common.lang.ClassUtils;
import com.ljg.panda.common.lang.LoggingCallable;
import com.ljg.panda.common.settings.ConfigUtils;
import com.ljg.panda.kafka.util.ConsumeDataIterator;
import com.ljg.panda.lambda.AbstractSparkLayer;
import com.ljg.panda.lambda.UpdateOffsetsFn;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.UUID;

/**
 * Main entry point for panda Speed Layer.
 * Speed Layer的入口实现，主要的功能包括：
 * 1、实时的消费Kafka中的updateTopic中的模型数据(Batch Layer发送过来的)
 * 根据这些模型数据来更新Speed Layer中的模型
 * 2、实时接收Kafka中的消息记录，每个批次根据这些新接收的消息记录来更新模型
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
public final class SpeedLayer<K, M, U> extends AbstractSparkLayer<K, M> {

    private static final Logger log = LoggerFactory.getLogger(SpeedLayer.class);

    private final String updateBroker; // Kafka的Broker Server的地址
    private final String updateTopic; // Model更新的topic
    private final int maxMessageSize; // Kafka Consumer单次从一个分区中拉取的最大的消息
    private final String modelManagerClassName; // 通过配置将Model管理者的具体实现传进来
    private final Class<? extends Deserializer<U>> updateDecoderClass; // Kafka Consumer 消费消息的value的解码器
    private JavaStreamingContext streamingContext; // Speed Layer中的StreamingContext
    private CloseableIterator<KeyMessage<String, U>> consumerIterator; // 像遍历Iterator一样去消费Kafka中的消息
    private SpeedModelManager<K, M, U> modelManager; // 根据modelManagerClassName生成的ModelManager的具体对象

    @SuppressWarnings("unchecked")
    public SpeedLayer(Config config) {
        super(config);
        this.updateBroker = config.getString("panda.update-topic.broker");
        this.updateTopic = config.getString("panda.update-topic.message.topic");
        this.maxMessageSize = config.getInt("panda.update-topic.message.max-size");
        this.modelManagerClassName = config.getString("panda.speed.model-manager-class");
        this.updateDecoderClass = (Class<? extends Deserializer<U>>) ClassUtils.loadClass(
                config.getString("panda.update-topic.message.decoder-class"), Deserializer.class);
        Preconditions.checkArgument(maxMessageSize > 0);
    }

    @Override
    protected String getConfigGroup() {
        return "speed";
    }

    @Override
    protected String getLayerName() {
        return "SpeedLayer";
    }

    /**
     * Speed层的启动方法
     */
    public synchronized void start() {
        String id = getID();
        if (id != null) {
            log.info("Starting Speed Layer {}", id);
        }
        // 初始化StreamingContext
        streamingContext = buildStreamingContext();
        log.info("Creating message stream from topic");
        // 初始化KafkaDStream
        JavaInputDStream<ConsumerRecord<K, M>> kafkaDStream = buildInputDStream(streamingContext);
        // 将接受到的Kafka中的消息转换成二元组，即kafkaDStream -> key-value类型的DStream
        JavaPairDStream<K, M> pairDStream =
                kafkaDStream.mapToPair((PairFunction<ConsumerRecord<K,M>, K, M>) mAndM -> new Tuple2<K, M>(mAndM.key(), mAndM.value()));

        modelManager = loadManagerInstance();

        // 1、Speed层中更新模型的topic的消费者
        KafkaConsumer<String, U> consumer = new KafkaConsumer<>(
                ConfigUtils.keyValueToProperties(
                        "group.id", "pandaGroup-" + getLayerName() + "-" + UUID.randomUUID(),
                        "bootstrap.servers", updateBroker,
                        "max.partition.fetch.bytes", maxMessageSize,
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", updateDecoderClass.getName(),
                        // Do start from the beginning of the update queue
                        "auto.offset.reset", "earliest"
                ));
        consumer.subscribe(Collections.singletonList(updateTopic));
        consumerIterator = new ConsumeDataIterator<>(consumer);

        // 加载并构建ModelManager实例，并且消费更新模型的topic的数据
        Configuration hadoopConf = streamingContext.sparkContext().hadoopConfiguration();
        new Thread(LoggingCallable.log(() -> {
            try {
                // 消费从Batch Layer发送过来的模型数据，然后更新Speed Layer的模型数据
                modelManager.consume(consumerIterator, hadoopConf);
            } catch (Throwable t) {
                log.error("Error while consuming updates", t);
                close();
            }
        }).asRunnable(), "pandaSpeedLayerUpdateConsumerThread").start();


        // 2、Speed层接收到新的原始数据后对内存中的Model进行更新
        pairDStream.foreachRDD(new SpeedLayerUpdate<>(modelManager, updateBroker, updateTopic));

        // Must use the raw Kafka stream to get offsets
        // 将消费了的最新的offsets保存到ZK中
        kafkaDStream.foreachRDD(new UpdateOffsetsFn<>(getGroupID(), getInputTopicLockMaster()));

        log.info("Starting Spark Streaming");

        streamingContext.start();
    }

    public void await() throws InterruptedException {
        Preconditions.checkState(streamingContext != null);
        log.info("Spark Streaming is running");
        streamingContext.awaitTermination();
    }

    @Override
    public synchronized void close() {
        if (modelManager != null) {
            log.info("Shutting down model manager");
            modelManager.close();
            modelManager = null;
        }
        if (consumerIterator != null) {
            log.info("Shutting down consumer");
            consumerIterator.close();
            consumerIterator = null;
        }
        if (streamingContext != null) {
            log.info("Shutting down Spark Streaming; this may take some time");
            streamingContext.stop(true, true);
            streamingContext = null;
        }
    }

    /**
     * 加载 SpeedModelManager 的实现类，并且构建SpeedModelManager实现类的对象
     * @return
     */
    @SuppressWarnings("unchecked")
    private SpeedModelManager<K, M, U> loadManagerInstance() {
        Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);

        // Java版本的SpeedModelManager实现类的加载和构建对象
        if (SpeedModelManager.class.isAssignableFrom(managerClass)) {

            try {
                return ClassUtils.loadInstanceOf(
                        modelManagerClassName,
                        SpeedModelManager.class,
                        new Class<?>[]{Config.class},
                        new Object[]{getConfig()});
            } catch (IllegalArgumentException iae) {
                return ClassUtils.loadInstanceOf(modelManagerClassName, SpeedModelManager.class);
            }

        } else {
            throw new IllegalArgumentException("Bad manager class: " + managerClass);
        }
    }

}
