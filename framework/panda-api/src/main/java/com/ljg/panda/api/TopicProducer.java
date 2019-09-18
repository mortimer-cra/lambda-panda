package com.ljg.panda.api;

import java.io.Closeable;

/**
 * Wraps access to a message topic {@code Producer}, including logic to instantiate the
 * object. This is a wrapper that can be serialized and re-create the {@code Producer}
 * remotely.
 *
 * 向某个topic发送消息的Producer的包装类
 *
 * @param <K> key type to send
 * @param <M> message type to send
 */
public interface TopicProducer<K, M> extends Closeable {

    /**
     * 返回当前Producer将消息发送到的broker
     * @return broker(s) that the producer is sending to
     */
    String getUpdateBroker();

    /**
     * 返回这个Producer将消息发送的topic
     * @return topic that the producer is sending to
     */
    String getTopic();

    /**
     * 发送消息
     * @param key     key to send to the topic
     * @param message message to send with key to the topic
     */
    void send(K key, M message);


    @Override
    default void close() {
        // do nothing
    }

}
