package com.ljg.panda.lambda.serving;

import com.google.common.base.Preconditions;
import com.ljg.panda.api.KeyMessage;
import com.ljg.panda.api.TopicProducer;
import com.ljg.panda.api.serving.ServingModelManager;
import com.ljg.panda.common.collection.CloseableIterator;
import com.ljg.panda.common.lang.ClassUtils;
import com.ljg.panda.common.lang.LoggingCallable;
import com.ljg.panda.common.settings.ConfigUtils;
import com.ljg.panda.kafka.util.ConsumeDataIterator;
import com.ljg.panda.kafka.util.KafkaUtils;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.io.Closeable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * {@link ServletContextListener} that initializes a {@link ServingModelManager} at web
 * app startup time in the Serving Layer.
 *
 *  Serving层，当tomcat容器启动的时候，初始化{@link ServingModelManager}
 *
 * @param <K> type of key written to input topic
 * @param <M> type of value written to input topic
 * @param <U> type of update/model read from update topic
 */
@WebListener
public final class ModelManagerListener<K, M, U> implements ServletContextListener, Closeable {

    private static final Logger log = LoggerFactory.getLogger(ModelManagerListener.class);

    static final String MANAGER_KEY = ModelManagerListener.class.getName() + ".ModelManager";
    private static final String INPUT_PRODUCER_KEY =
            ModelManagerListener.class.getName() + ".InputProducer";

    private Config config;
    private String updateTopic;
    private int maxMessageSize;
    private String updateTopicLockMaster;
    private String updateTopicBroker;
    private boolean readOnly;
    private String inputTopic;
    private String inputTopicLockMaster;
    private String inputTopicBroker;
    private String modelManagerClassName;
    private Class<? extends Deserializer<U>> updateDecoderClass;
    private CloseableIterator<KeyMessage<String, U>> consumerIterator;
    private ServingModelManager<U> modelManager;
    private TopicProducer<K, M> inputProducer;

    @SuppressWarnings("unchecked")
    void init(ServletContext context) {
        // 从上下文中获取配置信息
        String serializedConfig = context.getInitParameter(ConfigUtils.class.getName() + ".serialized");
        Objects.requireNonNull(serializedConfig);
        this.config = ConfigUtils.deserialize(serializedConfig);
        this.updateTopic = config.getString("panda.update-topic.message.topic");
        this.maxMessageSize = config.getInt("panda.update-topic.message.max-size");
        this.updateTopicLockMaster = config.getString("panda.update-topic.lock.master");
        this.updateTopicBroker = config.getString("panda.update-topic.broker");
        this.readOnly = config.getBoolean("panda.serving.api.read-only");
        if (!readOnly) {
            this.inputTopic = config.getString("panda.input-topic.message.topic");
            this.inputTopicLockMaster = config.getString("panda.input-topic.lock.master");
            this.inputTopicBroker = config.getString("panda.input-topic.broker");
        }
        this.modelManagerClassName = config.getString("panda.serving.model-manager-class");
        this.updateDecoderClass = (Class<? extends Deserializer<U>>) ClassUtils.loadClass(
                config.getString("panda.update-topic.message.decoder-class"), Deserializer.class);
        Preconditions.checkArgument(maxMessageSize > 0);
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        log.info("ModelManagerListener initializing");
        ServletContext context = sce.getServletContext();
        // 初始化配置
        init(context);

        if (!readOnly) {
            Preconditions.checkArgument(KafkaUtils.topicExists(inputTopicLockMaster, inputTopic),
                    "Topic %s does not exist; did you create it?", inputTopic);
            Preconditions.checkArgument(KafkaUtils.topicExists(updateTopicLockMaster, updateTopic),
                    "Topic %s does not exist; did you create it?", updateTopic);
            inputProducer = new TopicProducerImpl<>(inputTopicBroker, inputTopic);
            context.setAttribute(INPUT_PRODUCER_KEY, inputProducer);
        }
        // Serving层更新模型的topic的消费者
        KafkaConsumer<String, U> consumer = new KafkaConsumer<>(
                ConfigUtils.keyValueToProperties(
                        "group.id", "pandaGroup-ServingLayer-" + UUID.randomUUID(),
                        "bootstrap.servers", updateTopicBroker,
                        "max.partition.fetch.bytes", maxMessageSize,
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", updateDecoderClass.getName(),
                        // Do start from the beginning of the update queue
                        "auto.offset.reset", "earliest",
                        // Be gentler on hosts that aren't connecting:
                        "reconnect.backoff.ms", "1000",
                        "reconnect.backoff.max.ms", "10000"
                ));
        consumer.subscribe(Collections.singletonList(updateTopic));
        consumerIterator = new ConsumeDataIterator<>(consumer);

        // 加载并且构建一个模型管理类对象
        modelManager = loadManagerInstance();
        CountDownLatch threadStartLatch = new CountDownLatch(1);
        new Thread(LoggingCallable.log(() -> {
            try {
                // Can we do better than a default Hadoop config? Nothing else provides it here
                Configuration hadoopConf = new Configuration();
                threadStartLatch.countDown();
                modelManager.consume(consumerIterator, hadoopConf);
            } catch (Throwable t) {
                log.error("Error while consuming updates", t);
            } finally {
                // Ideally we would shut down ServingLayer, but not clear how to plumb that through
                // without assuming this has been run from ServingLayer and not a web app deployment
                close();
            }
        }).asRunnable(), "pandaServingLayerUpdateConsumerThread").start();

        try {
            threadStartLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Failed to wait for pandaServingLayerUpdateConsumerThread; continue");
        }

        // Set the Model Manager in the Application scope
        context.setAttribute(MANAGER_KEY, modelManager);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.info("ModelManagerListener destroying");
        // Slightly paranoid; remove objects from app scope manually
        ServletContext context = sce.getServletContext();
        for (Enumeration<String> names = context.getAttributeNames(); names.hasMoreElements(); ) {
            context.removeAttribute(names.nextElement());
        }

        close();

        // Hacky, but prevents Tomcat from complaining that ZK's cleanup thread 'leaked' since
        // it has a short sleep at its end
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            // continue
        }
    }

    @Override
    public synchronized void close() {
        log.info("ModelManagerListener closing");
        if (modelManager != null) {
            log.info("Shutting down model manager");
            modelManager.close();
            modelManager = null;
        }
        if (inputProducer != null) {
            log.info("Shutting down input producer");
            inputProducer.close();
            inputProducer = null;
        }
        if (consumerIterator != null) {
            log.info("Shutting down consumer");
            consumerIterator.close();
            consumerIterator = null;
        }
    }

    @SuppressWarnings("unchecked")
    private ServingModelManager<U> loadManagerInstance() {
        Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);
        // Java版本的ServingModelManager
        if (ServingModelManager.class.isAssignableFrom(managerClass)) {

            try {
                return ClassUtils.loadInstanceOf(
                        modelManagerClassName,
                        ServingModelManager.class,
                        new Class<?>[]{Config.class},
                        new Object[]{config});
            } catch (IllegalArgumentException iae) {
                return ClassUtils.loadInstanceOf(modelManagerClassName, ServingModelManager.class);
            }

        }  else {
            throw new IllegalArgumentException("Bad manager class: " + managerClass);
        }
    }

}
