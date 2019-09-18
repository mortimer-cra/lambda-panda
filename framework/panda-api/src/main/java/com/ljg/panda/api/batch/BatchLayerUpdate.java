package com.ljg.panda.api.batch;

import com.ljg.panda.api.TopicProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * Implementations of this interface define the update process for an instance of
 * the batch layer. It specifies what happens in one batch update cycle. Given
 * the time, and access to both past and current data, it defines some update process
 * in Spark, which produces some output (e.g. a machine learning model in PMML).
 *
 * Batch层的更新逻辑的接口。
 * 实现这个接口的类需要定义每一个Batch的更新处理逻辑
 *
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
@FunctionalInterface
public interface BatchLayerUpdate<K,M,U> extends Serializable {

  /**
   *  更新处理过程
   *  输入为一个Batch新接收到的数据以及历史数据，
   *  经过我们自定义的更新处理逻辑，
   *  产生输出(这个输出可以是机器学习的模型)发往到Kafka中或者保存到HDFS中
   *
   * @param sparkContext Spark context
   * @param timestamp timestamp of current interval - 当前批次的开始时间戳
   * @param newData data that has arrived in current interval - 当前批次接收到的新数据
   * @param pastData all previously-known data (may be {@code null}) - 历史数据，可能为null
   * @param modelDirString String representation of path where models should be output, if desired
   *                       - 如果输出的模型数据量比较大，需要保存的话，则这个参数就是表示模型保存的路径
   * @param modelUpdateTopic topic to push models onto, if desired. Note that this may be {@code null}
   *  if the application is configured to not produce updates to a topic
   *                         - 更新之后的模型发往到Kafka中的topic，如果更新之后的模型不需要发往到Kafka，则这个topic可以为null
   * @throws IOException if an error occurs during execution of the update function - 更新过程如果发生错误抛出的异常
   * @throws InterruptedException if the caller is interrupted waiting for parallel tasks
   *  to complete - 如果调用这个接口用户打断了正在等待运行的任务的时候，则抛出这个异常
   */
  void runUpdate(JavaSparkContext sparkContext,
                 long timestamp,
                 JavaPairRDD<K, M> newData,
                 JavaPairRDD<K, M> pastData,
                 String modelDirString,
                 TopicProducer<String, U> modelUpdateTopic)
      throws IOException, InterruptedException;

}
