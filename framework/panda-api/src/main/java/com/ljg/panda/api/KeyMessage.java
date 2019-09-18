package com.ljg.panda.api;

import java.io.Serializable;

/**
 *  抽象表示从Kafka中接收到的一条消息
 * @param <K>   消息的Key的类型
 * @param <M>   消息的Value的类型
 */
public interface KeyMessage<K,M> extends Serializable {
    K getKey();

    M getMessage();
}
