package com.ljg.panda.api.speed;

import com.ljg.panda.api.KeyMessage;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * Convenience implementation of {@link SpeedModelManager} that provides default implementations.
 *
 * 对接口 {@link SpeedModelManager} 提供一个默认的抽象实现，使得客户端实现接口{@link SpeedModelManager}时少写冗余代码
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
public abstract class AbstractSpeedModelManager<K, M, U> implements SpeedModelManager<K, M, U> {

    private static final Logger log = LoggerFactory.getLogger(AbstractSpeedModelManager.class);

    @Override
    public void consume(Iterator<KeyMessage<String, U>> updateIterator, Configuration hadoopConf) throws IOException {
        // 循环处理接收到的更新模型的Topic中的数据
        while (updateIterator.hasNext()) {
            try {
                KeyMessage<String, U> km = updateIterator.next();
                String key = km.getKey();
                U message = km.getMessage();
                Objects.requireNonNull(key);
                // 真正处理一条消息
                consumeKeyMessage(key, message, hadoopConf);
            } catch (Exception e) {
                log.warn("Exception while processing message; continuing", e);
            }
        }
    }

    /**
     * Convenience method that is called by the default implementation of
     * {@link #consume(Iterator, Configuration)}, to process one key-message pair.
     * It does nothing, except log the message. This should generally be overridden
     * if and only if {@link #consume(Iterator, Configuration)} is not.
     *
     * {@link #consume(Iterator, Configuration)}的默认实现调用的方法
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
