package com.ljg.panda.api.speed;

import com.ljg.panda.api.KeyMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * speed layer. It is given a reference to stream of updates during initialization, and consumes
 * models and new input, updates in-memory state, and produces updates accordingly.
 * <p>
 * Speed层存在于内存中的模型的管理类
 * 接受从Batch层发送过来的模型更新的数据，然后更新自己的内存中的模型
 * 接受从客户发送过来的原始数据，然后再更新自己的内存中的模型，并且返回模型中更新了的数据
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
public interface SpeedModelManager<K, M, U> extends Closeable {

    /**
     * Called by the framework to initiate a continuous process of reading models, and reading
     * from the input topic and updating model state in memory, and issuing updates to the
     * update topic. This will be executed asynchronously and may block. Note that an exception
     * or error thrown from this method is fatal and shuts down processing.
     * <p>
     * 消费读取Kafka中的模型更新topic中的数据，然后更新Speed层内存中的模型状态
     *
     * @param updateIterator iterator to read models from - 从Kafka中读取过来的模型数据
     * @param hadoopConf     Hadoop context, which may be required for reading from HDFS
     *                       - Hadoop的配置，如果我们需要读取HDFS的时候，则需要这个配置
     * @throws IOException if an error occurs while reading updates
     */
    void consume(Iterator<KeyMessage<String, U>> updateIterator, Configuration hadoopConf) throws IOException;

    /**
     * Speed层根据实时接收到的数据来更新内存中的模型状态
     *
     * @param newData RDD of raw new data from the topic - 从topic中接收到的新的原始数据
     * @return updates to publish on the update topic - 返回模型中更新的数据
     * @throws IOException if an error occurs while building updates
     */
    Iterable<U> buildUpdates(JavaPairRDD<K, M> newData) throws IOException;


    @Override
    default void close() {
        // do nothing
    }

}
