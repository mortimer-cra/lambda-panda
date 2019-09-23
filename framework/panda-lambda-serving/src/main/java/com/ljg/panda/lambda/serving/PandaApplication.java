package com.ljg.panda.lambda.serving;

import com.ljg.panda.common.lang.ClassUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The single JAX-RS app for the serving-layer.
 */
@ApplicationPath("")
public final class PandaApplication extends Application {

    private static final Logger log = LoggerFactory.getLogger(PandaApplication.class);

    @Context
    private ServletContext servletContext;
    private Set<Class<?>> classes;

    /**
     *
     * @return user endpoint implementations from the package named in context init param
     * {@code com.ljg.panda.lambda.serving.PandaApplication.packages}.
     */
    @Override
    public Set<Class<?>> getClasses() {
        if (classes == null) {
            classes = doGetClasses();
        }
        return classes;
    }

    private Set<Class<?>> doGetClasses() {
        // 从上下文中获取Resources所在的位置
        String packages =
                servletContext.getInitParameter(PandaApplication.class.getName() + ".packages");
        log.info("Creating JAX-RS from endpoints in package(s) {}", packages);
        Objects.requireNonNull(packages);
        Set<Class<?>> classes = new HashSet<>();
        // 遍历且扫描指定的包内所有的类，找出包含Path、Produces以及Provider三个注解的类的Class
        for (String thePackage : packages.split(",")) {
            Reflections reflections = new Reflections(thePackage);
            classes.addAll(getClassesInPackageAnnotatedBy(thePackage, reflections, Path.class));
            classes.addAll(getClassesInPackageAnnotatedBy(thePackage, reflections, Produces.class));
            classes.addAll(getClassesInPackageAnnotatedBy(thePackage, reflections, Provider.class));
        }
        // Want to configure these globally, but not depend on Jersey, even though it's
        // what will be used in practice by the provided apps.
        // 额外加上指定的类
        for (String optionalJerseyClass : new String[]{
                "org.glassfish.jersey.message.DeflateEncoder",
                "org.glassfish.jersey.message.GZipEncoder",
                "org.glassfish.jersey.server.filter.EncodingFilter"}) {
            if (ClassUtils.classExists(optionalJerseyClass)) {
                classes.add(ClassUtils.loadClass(optionalJerseyClass));
            }
        }
        log.debug("Found JAX-RS resources: {}", classes);
        return classes;
    }

    /**
     *  利用反射从指定的包内拿到含有指定的Annotation的类Class
     * @param thePackage
     * @param reflections
     * @param annotation
     * @return
     */
    private static Collection<Class<?>> getClassesInPackageAnnotatedBy(
            String thePackage,
            Reflections reflections,
            Class<? extends Annotation> annotation) {
        // Filter classes actually in subpackages
        return reflections.getTypesAnnotatedWith(annotation).stream().
                filter(c -> c.getPackage().getName().equals(thePackage)).
                collect(Collectors.toList());
    }

}
