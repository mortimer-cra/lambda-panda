package com.ljg.panda.lambda;

import com.ljg.panda.common.collection.Pair;
import com.ljg.panda.kafka.util.KafkaUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Function that reads offset range from latest RDD in a streaming job, and updates
 * Zookeeper/Kafka with the latest offset consumed.
 *
 * 读取最近的RDD消费到的Kafka中的Topic的offset范围，然后将消费的最新的offset更新到Zookeeper中
 *
 * @param <T> unused
 */
public final class UpdateOffsetsFn<T> implements VoidFunction<JavaRDD<T>> {

    private static final Logger log = LoggerFactory.getLogger(UpdateOffsetsFn.class);

    private final String group;
    private final String inputTopicLockMaster;

    public UpdateOffsetsFn(String group, String inputTopicLockMaster) {
        this.group = group;
        this.inputTopicLockMaster = inputTopicLockMaster;
    }

    /**
     * @param javaRDD RDD whose underlying RDD must be an instance of {@code HasOffsetRanges},
     *                such as {@code KafkaRDD}
     */
    @Override
    public void call(JavaRDD<T> javaRDD) {
        // 读取最新RDD消费的最新的offset范围
        OffsetRange[] ranges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
        Map<Pair<String, Integer>, Long> newOffsets = new HashMap<>(ranges.length);
        for (OffsetRange range : ranges) {
            // 从每一个offset范围中计算出消费的对应的topic、对应的partition中的最新的offset
            newOffsets.put(new Pair<>(range.topic(), range.partition()), range.untilOffset());
        }
        log.info("Updating offsets: {}", newOffsets);
        // 将offset保存到zk中
        KafkaUtils.setOffsets(inputTopicLockMaster, group, newOffsets);
    }

}
