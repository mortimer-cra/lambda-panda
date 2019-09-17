package com.ljg.panda.common.lang;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *  使得 {@link ReadWriteLock} 可以返回{@link AutoLock}s
 *  以及和{@link AutoLock#autoLock()}一样暴露{@link #autoReadLock()} and {@link #autoWriteLock()}接口
 *
 * {@code
 *   ReadWriteLock lock = ...;
 *   ...
 *   AutoReadWriteLock autoLock = new AutoReadWriteLock(lock);
 *   // Not locked
 *   try (AutoLock al = autoLock.autoReadLock()) { // variable required but unused
 *     // Locked
 *     ...
 *   }
 *   // Not locked
 * }
 */
public class AutoReadWriteLock implements ReadWriteLock {
    private final AutoLock readLock;
    private final AutoLock writeLock;

    public AutoReadWriteLock() {
        this(new ReentrantReadWriteLock());
    }

    public AutoReadWriteLock(ReadWriteLock lock) {
        this.readLock = new AutoLock(lock.readLock());
        this.writeLock = new AutoLock(lock.writeLock());
    }

    @Override
    public AutoLock readLock() {
        return readLock;
    }

    @Override
    public AutoLock writeLock() {
        return writeLock;
    }

    /**
     * @return the {@link ReadWriteLock#readLock()}, locked
     */
    public AutoLock autoReadLock() {
        return readLock.autoLock();
    }

    /**
     * @return the {@link ReadWriteLock#writeLock()} ()}, locked
     */
    public AutoLock autoWriteLock() {
        return writeLock.autoLock();
    }

    @Override
    public String toString() {
        return "AutoReadWriteLock[read:" + readLock + ", write:" + writeLock + "]";
    }
}
