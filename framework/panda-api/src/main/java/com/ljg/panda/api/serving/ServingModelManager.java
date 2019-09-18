package com.ljg.panda.api.serving;

import com.ljg.panda.api.KeyMessage;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * serving layer. It is given a reference to a stream of updates during initialization,
 * and consumes models and updates from it, and updates in-memory state accordingly.
 *
 * Serving层存在于内存中的Model的管理接口
 * 实现这个接口的类，负责管理Serving层内存中的Model的状态
 * 接受Kafka中的Model更新的数据进行Model状态的更新
 * 对外提供Model的查询服务
 *
 * @param <U> type of update message read/written
 */
public interface ServingModelManager<U> extends Closeable {

    /**
     * Called by the framework to initiate a continuous process of reading models, and reading
     * from the input topic and updating model state in memory, and issuing updates to the
     * update topic. This will be executed asynchronously and may block. Note that an exception
     * or error thrown from this method is fatal and shuts down processing.
     *
     * 消费读取Kafka中的模型更新topic中的数据，然后更新Serving层内存中的模型状态
     *
     * @param updateIterator iterator to read models from
     * @param hadoopConf     Hadoop context, which may be required for reading from HDFS
     * @throws IOException if an error occurs while reading updates
     */
    void consume(Iterator<KeyMessage<String, U>> updateIterator, Configuration hadoopConf) throws IOException;

    /**
     * @return configuration for the serving layer
     */
    Config getConfig();

    /**
     * 获取Serving层内存中的模型
     * @return in-memory model representation
     */
    ServingModel getModel();

    /**
     * @return true iff the model is considered read-only and not updateable
     */
    boolean isReadOnly();


    @Override
    default void close() {
        // do nothing
    }

}
