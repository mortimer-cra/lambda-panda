package com.ljg.panda.common.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 *  JVM相关的工具方法
 */
public final class JVMUtils {

    private static final Logger log = LoggerFactory.getLogger(JVMUtils.class);

    private static final PandaShutdownHook SHUTDOWN_HOOK = new PandaShutdownHook();

    private JVMUtils() {}

    /**
     *  在虚拟机关闭的时候close掉所有在SHUTDOWN_HOOK中注册的Closeable
     * @param closeable
     */
    public static void closeAtShutdown(Closeable closeable) {
        if (SHUTDOWN_HOOK.addCloseable(closeable)) {
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK, "PandaShutdownHookThread"));
            } catch (IllegalStateException ise) {
                log.warn("Can't close {} at shutdown since shutdown is in progress", closeable);
            }
        }
    }

    /**
     *  获取已经使用了的对内存(单位是bytes)
     * @return
     */
    public static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
