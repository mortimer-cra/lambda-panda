package com.ljg.panda.common.lang;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * 速率控制器
 */
public final class RateLimitCheck {

    private final long intervalNanos;
    private long nextSuccess;

    public RateLimitCheck(long time, TimeUnit unit) {
        Preconditions.checkArgument(time > 0);
        intervalNanos = TimeUnit.NANOSECONDS.convert(time, unit);
        nextSuccess = System.nanoTime();
    }

    /**
     *  一开始返回true
     *  然后在intervalNanos时间内返回false
     *  超过intervalNanos后返回true
     *  然后又开始返回false
     *  用于速率控制
     * @return
     */
    public boolean test() {
        boolean test = false;
        long now = System.nanoTime();
        synchronized (this) {
            if (now >= nextSuccess) {
                test = true;
                nextSuccess = now + intervalNanos;
            }
        }
        return test;
    }
}
