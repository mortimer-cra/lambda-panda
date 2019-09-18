package com.ljg.panda.api.serving;


import com.ljg.panda.api.TopicProducer;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import java.util.Objects;

/**
 * A utility class that can serve as a superclass of Serving Layer application endpoints.
 * It handles loading provided objects like a {@link ServingModelManager}.
 * Serving层中的restful接口的父类(包含了接口需要的公共的代码)
 */
public abstract class PandaResource {

    public static final String MODEL_MANAGER_KEY =
            "com.ljg.panda.lambda.serving.ModelManagerListener.ModelManager";
    public static final String INPUT_PRODUCER_KEY =
            "com.ljg.panda.lambda.serving.ModelManagerListener.InputProducer";

    @Context
    private ServletContext servletContext;
    private ServingModelManager<?> servingModelManager;
    private TopicProducer<?, ?> inputProducer;

    @SuppressWarnings("unchecked")
    @PostConstruct
    protected void init() {
        servingModelManager = Objects.requireNonNull(
                (ServingModelManager<?>) servletContext.getAttribute(MODEL_MANAGER_KEY),
                "No ServingModelManager");
        inputProducer = Objects.requireNonNull(
                (TopicProducer<?, ?>) servletContext.getAttribute(INPUT_PRODUCER_KEY),
                "No input producer available; read-only mode?");
    }

    /**
     *  返回ServingModelManager
     * @return a reference to the {@link ServingModelManager} for the app, configured in the
     * {@link ServletContext} under key {@link #MODEL_MANAGER_KEY}
     */
    protected final ServingModelManager<?> getServingModelManager() {
        return servingModelManager;
    }

    /**
     * 返回TopicProducer
     * @return a reference to the {@link TopicProducer} for the app, configured in the
     * {@link ServletContext} under key {@link #INPUT_PRODUCER_KEY}
     */
    protected final TopicProducer<?, ?> getInputProducer() {
        return inputProducer;
    }

}
