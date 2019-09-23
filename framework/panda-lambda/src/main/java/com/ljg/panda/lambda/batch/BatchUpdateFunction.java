package com.ljg.panda.lambda.batch;

import com.ljg.panda.api.TopicProducer;
import com.ljg.panda.api.batch.BatchLayerUpdate;
import com.ljg.panda.common.settings.ConfigUtils;
import com.ljg.panda.lambda.TopicProducerImpl;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Framework for executing the batch layer update, and storing data to persistent storage,
 * in the context of a streaming framework.
 *  Batch Layer执行模型训练的框架，主要做如下的事情：
 *  1、从HDFS中读取历史接收到的消息记录数据
 *  2、将历史消息记录和新接收到的消息记录喂给自定义的BatchLayerUpdate进行特定的模型训练和更新
 *  3、并决定是否需要将更新的模型发往到Kafka，通知Speed层和Serving层
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
final class BatchUpdateFunction<K, M, U> implements VoidFunction2<JavaPairRDD<K, M>, Time> {

    private static final Logger log = LoggerFactory.getLogger(BatchUpdateFunction.class);

    private final Class<K> keyClass; // 消息记录的Key的类型
    private final Class<M> messageClass; // 消息记录Value的类型
    private final Class<? extends Writable> keyWritableClass; // 读取HDFS中历史消息记录的Key的类型
    private final Class<? extends Writable> messageWritableClass; // 读取HDFS中历史消息记录的Value的类型
    private final String dataDirString; // 消息记录的存储的位置
    private final String modelDirString; // 模型数据存储的位置
    private final BatchLayerUpdate<K, M, U> updateInstance; // Batch层的更新逻辑的具体实现对象
    private final String updateBroker; // Kafka Broker Server的地址
    private final String updateTopic; // Kafka 更新模型的Topic
    private final JavaSparkContext sparkContext; // SparkContext

    BatchUpdateFunction(Config config,
                        Class<K> keyClass,
                        Class<M> messageClass,
                        Class<? extends Writable> keyWritableClass,
                        Class<? extends Writable> messageWritableClass,
                        String dataDirString,
                        String modelDirString,
                        BatchLayerUpdate<K, M, U> updateInstance,
                        JavaStreamingContext streamingContext) {
        this.keyClass = keyClass;
        this.messageClass = messageClass;
        this.keyWritableClass = keyWritableClass;
        this.messageWritableClass = messageWritableClass;
        this.dataDirString = dataDirString;
        this.modelDirString = modelDirString;
        this.updateBroker = ConfigUtils.getOptionalString(config, "panda.update-topic.broker");
        this.updateTopic = ConfigUtils.getOptionalString(config, "panda.update-topic.message.topic");
        this.updateInstance = updateInstance;
        this.sparkContext = streamingContext.sparkContext();
    }

    @Override
    public void call(JavaPairRDD<K, M> newData, Time timestamp)
            throws IOException, InterruptedException {

        if (newData.isEmpty()) {
            log.info("No data in current generation's RDD; nothing to do");
            return;
        }

        log.info("Beginning update at {}", timestamp);

        Configuration hadoopConf = sparkContext.hadoopConfiguration();
        if (hadoopConf.getResource("core-site.xml") == null) {
            log.warn("Hadoop config like core-site.xml was not found; " +
                    "is the Hadoop config directory on the classpath?");
        }

        // 从HDFS中读取历史原始数据
        JavaPairRDD<K, M> pastData;
        Path inputPathPattern = new Path(dataDirString + "/*/part-*");
        FileSystem fs = FileSystem.get(inputPathPattern.toUri(), hadoopConf);
        FileStatus[] inputPathStatuses = fs.globStatus(inputPathPattern);
        if (inputPathStatuses == null || inputPathStatuses.length == 0) {

            log.info("No past data at path(s) {}", inputPathPattern);
            pastData = null;

        } else {

            log.info("Found past data at path(s) like {}", inputPathStatuses[0].getPath());
            Configuration updatedConf = new Configuration(hadoopConf);
            updatedConf.set(FileInputFormat.INPUT_DIR, joinFSPaths(fs, inputPathStatuses));
            // 读取HDFS中的数据，生成key和value都是Writable类型的RDD
            @SuppressWarnings("unchecked")
            JavaPairRDD<Writable, Writable> pastWritableData = (JavaPairRDD<Writable, Writable>)
                    sparkContext.newAPIHadoopRDD(updatedConf,
                            SequenceFileInputFormat.class,
                            keyWritableClass,
                            messageWritableClass);
            // 将key和value都是Writable类型的RDD转换成key为指定的keyClass类型以及value为valueClass类型的RDD
            pastData = pastWritableData.mapToPair(
                    new WritableToValueFunction<>(keyClass,
                            messageClass,
                            keyWritableClass,
                            messageWritableClass));
        }

        // 没有配置updateTopic的话，则不将更新后的model发往kafka中
        if (updateTopic == null || updateBroker == null) {
            log.info("Not producing updates to update topic since none was configured");
            updateInstance.runUpdate(sparkContext,
                    timestamp.milliseconds(),
                    newData,
                    pastData,
                    modelDirString,
                    null);
        } else {
            // This TopicProducer should not be async; sends one big model generally and
            // needs to occur before other updates reliably rather than be buffered
            // 更新model，且将更新后的model发往kafka，供Speed层和Serving层消费
            try (TopicProducer<String, U> producer =
                         new TopicProducerImpl<>(updateBroker, updateTopic, false)) {
                updateInstance.runUpdate(sparkContext,
                        timestamp.milliseconds(),
                        newData,
                        pastData,
                        modelDirString,
                        producer);
            }
        }
    }

    /**
     * 将数组类型的Path拼接成逗号分隔开的Path的字符串
     * @return paths from {@link FileStatus}es into one comma-separated String
     * @see FileInputFormat#addInputPath(org.apache.hadoop.mapreduce.Job, Path)
     */
    private static String joinFSPaths(FileSystem fs, FileStatus[] statuses) {
        StringBuilder joined = new StringBuilder();
        for (FileStatus status : statuses) {
            if (joined.length() > 0) {
                joined.append(',');
            }
            // 保证Path是标准的HDFS路径，即含有schema（比如hdfs://master:9999/....）
            Path path = fs.makeQualified(status.getPath());
            joined.append(StringUtils.escapeString(path.toString()));
        }
        return joined.toString();
    }

}
