package com.ljg.panda.lambda.batch;

import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Translates a key and message in {@link Writable}s format to value objects.
 *  将Writable类型的key和value转换成指定的类型的key和value对象
 * @see ValueToWritableFunction
 */
final class WritableToValueFunction<K,M>
    implements PairFunction<Tuple2<Writable,Writable>,K,M> {

  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;
  private transient ValueWritableConverter<K> keyConverter;
  private transient ValueWritableConverter<M> messageConverter;

  WritableToValueFunction(Class<K> keyClass,
                          Class<M> messageClass,
                          Class<? extends Writable> keyWritableClass,
                          Class<? extends Writable> messageWritableClass) {
    this.keyClass = keyClass;
    this.messageClass = messageClass;
    this.keyWritableClass = keyWritableClass;
    this.messageWritableClass = messageWritableClass;
    initConverters();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initConverters();
  }

  private void initConverters() {
    keyConverter = new ValueWritableConverter<>(keyClass, keyWritableClass);
    messageConverter = new ValueWritableConverter<>(messageClass, messageWritableClass);
  }

  @Override
  public Tuple2<K,M> call(Tuple2<Writable,Writable> keyMessage) {
    return new Tuple2<>(keyConverter.fromWritable(keyMessage._1()),
                        messageConverter.fromWritable(keyMessage._2()));
  }

}
