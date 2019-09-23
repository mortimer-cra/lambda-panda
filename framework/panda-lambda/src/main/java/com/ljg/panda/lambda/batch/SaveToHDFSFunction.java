package com.ljg.panda.lambda.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Function that saves RDDs to HDFS -- only if they're non empty, to prevent creation
 * of many small empty files if data is infrequent but the model interval is short.
 *
 * 将RDDs保存到HDFS的函数
 * 为了防止生成很多的小文件，对于没有接收到数据的RDD则不进行保存
 */
final class SaveToHDFSFunction<K, M> implements VoidFunction2<JavaPairRDD<K, M>, Time> {

    private static final Logger log = LoggerFactory.getLogger(SaveToHDFSFunction.class);

    private final String prefix;
    private final String suffix;
    private final Class<K> keyClass;
    private final Class<M> messageClass;
    private final Class<? extends Writable> keyWritableClass;
    private final Class<? extends Writable> messageWritableClass;
    private final Configuration hadoopConf;

    SaveToHDFSFunction(String prefix,
                       String suffix,
                       Class<K> keyClass,
                       Class<M> messageClass,
                       Class<? extends Writable> keyWritableClass,
                       Class<? extends Writable> messageWritableClass,
                       Configuration hadoopConf) {
        this.prefix = prefix;
        this.suffix = suffix;
        this.keyClass = keyClass;
        this.messageClass = messageClass;
        this.keyWritableClass = keyWritableClass;
        this.messageWritableClass = messageWritableClass;
        this.hadoopConf = hadoopConf;
    }

    @Override
    public void call(JavaPairRDD<K, M> rdd, Time time) throws IOException {
        if (rdd.isEmpty()) {
            log.info("RDD was empty, not saving to HDFS");
        } else {
            // 保存的文件名
            String file = prefix + "-" + time.milliseconds() + "." + suffix;
            Path path = new Path(file);
            FileSystem fs = FileSystem.get(path.toUri(), hadoopConf);
            if (fs.exists(path)) {
                log.warn("Saved data already existed, possibly from a failed job. Deleting {}", path);
                fs.delete(path, true);
            }
            log.info("Saving RDD to HDFS at {}", file);
            // 将RDD的key和value的类型转成Writable类型，并且以Sequence File的格式保存在HDFS中
            rdd.mapToPair(
                    new ValueToWritableFunction<>(keyClass, messageClass, keyWritableClass, messageWritableClass)
            ).saveAsNewAPIHadoopFile(
                    file,
                    keyWritableClass,
                    messageWritableClass,
                    SequenceFileOutputFormat.class,
                    hadoopConf);
        }
    }
}
