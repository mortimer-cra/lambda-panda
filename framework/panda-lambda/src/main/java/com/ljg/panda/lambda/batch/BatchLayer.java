package com.ljg.panda.lambda.batch;

import com.google.common.base.Preconditions;
import com.ljg.panda.api.batch.BatchLayerUpdate;
import com.ljg.panda.common.lang.ClassUtils;
import com.ljg.panda.lambda.AbstractSparkLayer;
import com.ljg.panda.lambda.UpdateOffsetsFn;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.regex.Pattern;

/**
 * Main entry point for Panda Batch Layer.
 * Batch Layer的入口实现，包含的功能如下：
 * 1、实时接收Kafka中的数据，并且进行转换
 * 2、将每一个批次接收到的新数据存储在HDFS中
 * 3、将最新批次接收到的新数据以及历史批次接收到的数据(存储在HDFS上)作为模型的训练数据集，
 * 来训练出新的模型，并且将新的模型替代存储在HDFS上的老的模型
 * 4、将更新了之后的模型通过Kafka通知到Speed Layer和Serving Layer
 * 5、将每一个批次消费到的Kafka的topic的offsets保存到zookeeper中，防止数据的丢失
 * 6、维护消息记录以及模型数据的存储时间周期
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
public final class BatchLayer<K, M, U> extends AbstractSparkLayer<K, M> {

    private static final Logger log = LoggerFactory.getLogger(BatchLayer.class);

    private static final int NO_MAX_AGE = -1;

    private final Class<? extends Writable> keyWritableClass; // 将数据存储到HDFS上时key的Writable类型
    private final Class<? extends Writable> messageWritableClass; // 将数据存储到HDFS上时value的Writable类型
    private final String updateClassName; // 从配置中传进来的Batch Layer的Update的具体实现的类名
    private final String dataDirString; // 从Kafka中接收到的消息的存储的HDFS地址
    private final String modelDirString; // 训练出的模型的存储的HDFS地址
    private final int maxDataAgeHours; // 消息存储的最大的时间
    private final int maxModelAgeHours; // 模型存储的最大的时间
    private JavaStreamingContext streamingContext; // Batch Layer的StreamingContext

    public BatchLayer(Config config) {
        super(config);
        this.keyWritableClass = ClassUtils.loadClass(
                config.getString("panda.batch.storage.key-writable-class"), Writable.class);
        this.messageWritableClass = ClassUtils.loadClass(
                config.getString("panda.batch.storage.message-writable-class"), Writable.class);
        this.updateClassName = config.getString("panda.batch.update-class"); // Batch层更新model的实现类的名称
        this.dataDirString = config.getString("panda.batch.storage.data-dir"); // 原始数据存储的位置
        this.modelDirString = config.getString("panda.batch.storage.model-dir"); // model存储的位置
        this.maxDataAgeHours = config.getInt("panda.batch.storage.max-age-data-hours"); // 原始数据存储最长的时间
        this.maxModelAgeHours = config.getInt("panda.batch.storage.max-age-model-hours"); //model存储最长的时间
        Preconditions.checkArgument(!dataDirString.isEmpty());
        Preconditions.checkArgument(!modelDirString.isEmpty());
        Preconditions.checkArgument(maxDataAgeHours >= 0 || maxDataAgeHours == NO_MAX_AGE);
        Preconditions.checkArgument(maxModelAgeHours >= 0 || maxModelAgeHours == NO_MAX_AGE);
    }

    @Override
    protected String getConfigGroup() {
        return "batch";
    }

    @Override
    protected String getLayerName() {
        return "BatchLayer";
    }

    /**
     * 启动Batch层的方法
     */
    public synchronized void start() {
        String id = getID();
        if (id != null) {
            log.info("Starting Batch Layer {}", id);
        }
        // 1、初始化StreamingContext
        streamingContext = buildStreamingContext();
        JavaSparkContext sparkContext = streamingContext.sparkContext();
        Configuration hadoopConf = sparkContext.hadoopConfiguration();

        // 2、设置checkpoint路径
        Path checkpointPath = new Path(new Path(modelDirString), ".checkpoint");
        log.info("Setting checkpoint dir to {}", checkpointPath);
        sparkContext.setCheckpointDir(checkpointPath.toString());

        // 3、初始化kafkaDStream
        log.info("Creating message stream from topic");
        JavaInputDStream<ConsumerRecord<K, M>> kafkaDStream = buildInputDStream(streamingContext);
        // 4、将接受到的Kafka中的消息转换成二元组，即kafkaDStream -> key-value类型的DStream
        JavaPairDStream<K,M> pairDStream =
                kafkaDStream.mapToPair((PairFunction<ConsumerRecord<K,M>, K, M>) mAndM ->
                        new Tuple2<K, M>(mAndM.key(), mAndM.value()));

        Class<K> keyClass = getKeyClass();
        Class<M> messageClass = getMessageClass();
        // Batch层的model更新
        pairDStream.foreachRDD(
                new BatchUpdateFunction<>(getConfig(),
                        keyClass,
                        messageClass,
                        keyWritableClass,
                        messageWritableClass,
                        dataDirString,
                        modelDirString,
                        loadUpdateInstance(),
                        streamingContext));

        // "Inline" saveAsNewAPIHadoopFiles to be able to skip saving empty RDDs
        // 将接收到的原始数据保存在HDFS上
        pairDStream.foreachRDD(new SaveToHDFSFunction<>(
                dataDirString + "/panda",
                "data",
                keyClass,
                messageClass,
                keyWritableClass,
                messageWritableClass,
                hadoopConf));

        // Must use the raw Kafka stream to get offsets
        // 将消费的最新的offset保存到zk中
        kafkaDStream.foreachRDD(new UpdateOffsetsFn<>(getGroupID(), getInputTopicLockMaster()));

        // 删除指定时间前的数据
        if (maxDataAgeHours != NO_MAX_AGE) {
            pairDStream.foreachRDD(new DeleteOldDataFn<>(hadoopConf,
                    dataDirString,
                    Pattern.compile("-(\\d+)\\."),
                    maxDataAgeHours));
        }
        // 删除指定时间前的model
        if (maxModelAgeHours != NO_MAX_AGE) {
            pairDStream.foreachRDD(new DeleteOldDataFn<>(hadoopConf,
                    modelDirString,
                    Pattern.compile("(\\d+)"),
                    maxModelAgeHours));
        }

        log.info("Starting Spark Streaming");

        streamingContext.start();
    }

    public void await() throws InterruptedException {
        JavaStreamingContext theStreamingContext;
        synchronized (this) {
            theStreamingContext = streamingContext;
            Preconditions.checkState(theStreamingContext != null);
        }
        log.info("Spark Streaming is running");
        theStreamingContext.awaitTermination(); // Can't do this with lock
    }

    @Override
    public synchronized void close() {
        if (streamingContext != null) {
            log.info("Shutting down Spark Streaming; this may take some time");
            streamingContext.stop(true, true);
            streamingContext = null;
        }
    }

    /**
     *  加载BatchLayerUpdate的指定具体实现类，并且创建实现类的对象
     * @return
     */
    @SuppressWarnings("unchecked")
    private BatchLayerUpdate<K, M, U> loadUpdateInstance() {
        // 加载BatchLayerUpdate的实现类
        Class<?> updateClass = ClassUtils.loadClass(updateClassName);
        // Java实现的BatchLayerUpdate
        if (BatchLayerUpdate.class.isAssignableFrom(updateClass)) {

            try {
                return ClassUtils.loadInstanceOf(
                        updateClassName,
                        BatchLayerUpdate.class,
                        new Class<?>[]{Config.class},
                        new Object[]{getConfig()});
            } catch (IllegalArgumentException iae) {
                return ClassUtils.loadInstanceOf(updateClassName, BatchLayerUpdate.class);
            }

        }  else {
            throw new IllegalArgumentException("Bad update class: " + updateClassName);
        }
    }

}
