package com.ljg.panda.common.lang;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *  将Lock和AutoCloseable绑在一起，使得可以使用try-with-resources的方式使用Lock
 *
 *  使用方式如下：
 * {@code
 *   Lock lock = ...;
 *   ...
 *   AutoLock autoLock = new AutoLock(lock);
 *   // Not locked
 *   try (AutoLock al = autoLock.autoLock()) { // variable required but unused
 *     // Locked
 *     ...
 *   }
 *   // Not locked
 * }
 */
public final class AutoLock implements AutoCloseable, Lock {

    private final Lock lock;

    public AutoLock() {
        this(new ReentrantLock());
    }

    public AutoLock(Lock lock) {
        Objects.requireNonNull(lock);
        this.lock = lock;
    }

    public AutoLock autoLock() {
        lock();
        return this;
    }

    @Override
    public void close() {
        unlock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public Condition newCondition() {
        return lock.newCondition();
    }

    @Override
    public String toString() {
        return "AutoLock:" + lock;
    }
}
