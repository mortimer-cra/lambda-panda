package com.ljg.panda.api.serving;


import com.ljg.panda.api.KeyMessage;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * Convenience implementation of {@link ServingModelManager} that provides several default implementations.
 *
 * {@link ServingModelManager} 的抽象实现
 *
 * @param <U> type of update message read/written
 */
public abstract class AbstractServingModelManager<U> implements ServingModelManager<U> {

    private static final Logger log = LoggerFactory.getLogger(AbstractServingModelManager.class);

    private final Config config;
    private final boolean readOnly;

    /**
     * @param config Panda {@link Config} object
     */
    protected AbstractServingModelManager(Config config) {
        this.config = config;
        this.readOnly = config.getBoolean("panda.serving.api.read-only");
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void consume(Iterator<KeyMessage<String, U>> updateIterator, Configuration hadoopConf) throws IOException {
        while (updateIterator.hasNext()) {
            try {
                KeyMessage<String, U> km = updateIterator.next();
                String key = km.getKey();
                U message = km.getMessage();
                Objects.requireNonNull(key);
                consumeKeyMessage(key, message, hadoopConf);
            } catch (Throwable t) {
                log.warn("Error while processing message; continuing", t);
            }
        }
    }

    /**
     * Convenience method that is called by the default implementation of
     * {@link #consume(Iterator, Configuration)}, to process one key-message pair.
     * It does nothing, except log the message. This should generally be overridden
     * if and only if {@link #consume(Iterator, Configuration)} is not.
     *
     * 处理从Kafka中接收到的每一条消息
     * 默认实现只是简单的打印日志
     *
     * @param key        key to process (non-null)
     * @param message    message to process
     * @param hadoopConf Hadoop configuration for process
     * @throws IOException if an error occurs while processing the message
     */
    public void consumeKeyMessage(String key, U message, Configuration hadoopConf) throws IOException {
        log.info("{} : {}", key, message);
    }

}
