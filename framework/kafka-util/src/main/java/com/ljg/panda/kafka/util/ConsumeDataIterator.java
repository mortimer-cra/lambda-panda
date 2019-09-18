package com.ljg.panda.kafka.util;


import com.google.common.collect.AbstractIterator;
import com.ljg.panda.api.KeyMessage;
import com.ljg.panda.api.KeyMessageImpl;
import com.ljg.panda.common.collection.CloseableIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Iterator;
import java.util.Objects;

/**
 * An iterator over records in a Kafka topic.
 * 从Kafka的某个topic的Consumer中构建的一个iterator
 * 使得我们可以使用iterator的模式来消费Kafka中的数据
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ConsumeDataIterator<K, V>
        extends AbstractIterator<KeyMessage<K, V>>
        implements CloseableIterator<KeyMessage<K, V>> {

    private static final long MIN_POLL_MS = 1;
    private static final long MAX_POLL_MS = 1000;

    private final KafkaConsumer<K, V> consumer;
    private volatile Iterator<ConsumerRecord<K, V>> iterator;
    private volatile boolean closed;

    public ConsumeDataIterator(KafkaConsumer<K, V> consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    /**
     *  消费Kafka中下一条消息
     * @return
     */
    @Override
    protected KeyMessage<K, V> computeNext() {
        if (iterator == null || !iterator.hasNext()) {
            try {
                long timeout = MIN_POLL_MS;
                ConsumerRecords<K, V> records;
                while ((records = consumer.poll(timeout)).isEmpty()) {
                    timeout = Math.min(MAX_POLL_MS, timeout * 2);
                }
                iterator = records.iterator();
            } catch (Exception e) {
                consumer.close();
                return endOfData();
            }
        }
        ConsumerRecord<K, V> mm = iterator.next();
        return new KeyMessageImpl<>(mm.key(), mm.value());
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            consumer.close();
        }
    }
}
