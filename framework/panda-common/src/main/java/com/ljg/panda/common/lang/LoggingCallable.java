package com.ljg.panda.common.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * 用于异步执行某个Task的时候出错了打印日志的类
 * @param <V>
 */
public final class LoggingCallable<V> implements Callable<V> {
    private static final Logger log = LoggerFactory.getLogger(LoggingCallable.class);

    private final Callable<V> delegate; // 需要执行的某个Task

    private LoggingCallable(Callable<V> delegate) {
        Objects.requireNonNull(delegate);
        this.delegate = delegate;
    }

    public static <V> LoggingCallable<V> log(Callable<V> delegate) {
        return new LoggingCallable<>(delegate);
    }

    public static LoggingCallable<Void> log(AllowExceptionSupplier delegate) {
        return log(() -> {
            delegate.get();
            return null;
        });
    }


    @Override
    public V call() throws Exception {
        try {
            //执行Task
            return delegate.call();
        } catch (Throwable t) { // 如果出错的话则打印日志，并且将异常抛出去
            log.warn("Unexpected error in {}", delegate, t);
            throw t;
        }
    }

    // 将task的执行包装成一个会打印日志的Runnable
    public Runnable asRunnable() {
        return () -> {
            try {
                delegate.call();
            } catch (Exception e) {
                log.warn("Unexpected error in {}", delegate, e);
                throw new IllegalStateException(e);
            } catch (Throwable t) {
                log.warn("Unexpected error in {}", delegate, t);
                throw t;
            }
        };
    }

    /**
     * Like {@link java.util.function.Supplier} but allows {@link Exception} from
     * {@link java.util.function.Supplier#get()}.
     */
    @FunctionalInterface
    public interface AllowExceptionSupplier {
        void get() throws Exception;
    }
}
