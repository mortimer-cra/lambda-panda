package com.ljg.panda.common.lang;

import com.google.common.base.Preconditions;
import com.ljg.panda.common.io.IOUtils;

import java.io.Closeable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

/**
 *  包含一个Closeable的列表，这些Closeable在虚拟机关闭的时候需要close的
 *  当虚拟机关闭的时候，会触发这个Runnable的run方法
 */
public class PandaShutdownHook implements Runnable {

    private final Deque<Closeable> closeAtShutdown = new LinkedList<>();
    private volatile boolean triggered;

    @Override
    public void run() {
        triggered = true;
        synchronized (closeAtShutdown) {
            closeAtShutdown.forEach(IOUtils::closeQuietly);
        }
    }

    /**
     *  将一个Closeable对象放到队列中
     * @param closeable
     * @return 如果这个Closeable对象是第一次放到队列的话，则返回true
     */
    public boolean addCloseable(Closeable closeable) {
        Objects.requireNonNull(closeable);
        Preconditions.checkState(!triggered, "Can't add closeable %s; already shutting down", closeable);
        synchronized (closeAtShutdown) {
            boolean wasFirst = closeAtShutdown.isEmpty();
            closeAtShutdown.push(closeable);
            return wasFirst;
        }
    }


}
