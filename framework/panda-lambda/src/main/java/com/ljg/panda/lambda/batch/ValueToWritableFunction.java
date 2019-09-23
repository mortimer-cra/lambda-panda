package com.ljg.panda.lambda.batch;

import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Translates a key and message value object into {@link Writable}s encapsulating the same
 * values, so that they may be serialized using Hadoop's framework.
 *
 * 将其他类型的key和value转成Writable类型的key和value
 *
 * @see WritableToValueFunction
 */
final class ValueToWritableFunction<K, M>
        implements PairFunction<Tuple2<K, M>, Writable, Writable> {

    private final Class<K> keyClass;
    private final Class<M> messageClass;
    private final Class<? extends Writable> keyWritableClass;
    private final Class<? extends Writable> messageWritableClass;
    private transient ValueWritableConverter<K> keyConverter;
    private transient ValueWritableConverter<M> messageConverter;

    ValueToWritableFunction(Class<K> keyClass,
                            Class<M> messageClass,
                            Class<? extends Writable> keyWritableClass,
                            Class<? extends Writable> messageWritableClass) {
        this.keyClass = keyClass;
        this.messageClass = messageClass;
        this.keyWritableClass = keyWritableClass;
        this.messageWritableClass = messageWritableClass;
        initConverters();
    }

    // 反序列化对象的时候，也需要初始化Converters
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initConverters();
    }

    private void initConverters() {
        keyConverter = new ValueWritableConverter<>(keyClass, keyWritableClass);
        messageConverter = new ValueWritableConverter<>(messageClass, messageWritableClass);
    }

    @Override
    public Tuple2<Writable, Writable> call(Tuple2<K, M> keyMessage) {
        return new Tuple2<Writable, Writable>(keyConverter.toWritable(keyMessage._1()),
                messageConverter.toWritable(keyMessage._2()));
    }

}
