package com.ljg.panda.common.random;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;

/**
 * 随机数生成管理器
 */
public final class RandomManager {

    private static final long TEST_SEED = getTestSeed();
    private static boolean useTestSeed;
    private static final Reference<? extends Collection<RandomGenerator>> INSTANCES =
            new SoftReference<>(new ArrayList<>());

    private RandomManager() {}

    private static long getTestSeed() {
        String seedString = System.getProperty("panda.test.seed", "1234567890123456789");
        try {
            return Long.parseLong(seedString);
        } catch (NumberFormatException nfe) {
            return Long.parseLong(seedString, 16);
        }
    }

    public static RandomGenerator getRandom() {
        if (useTestSeed) {
            // No need to track instances anymore
            return new Well19937c(TEST_SEED);
        }
        return getUnseededRandom();
    }

    static RandomGenerator getUnseededRandom() {
        RandomGenerator random = new Well19937c();
        Collection<RandomGenerator> instances = INSTANCES.get();
        if (instances != null) {
            synchronized (instances) {
                instances.add(random);
            }
        } // else oh well, only matters in tests
        return random;
    }

    /**
     * @param seed explicit seed for random number generator
     * @return a new, seeded {@link RandomGenerator}
     */
    public static RandomGenerator getRandom(long seed) {
        // Don't track these or use the test seed as the caller has manually specified
        // the seeding behavior
        return new Well19937c(seed);
    }

    /**
     * <em>Only call in test code.</em> Causes all known instances of {@link RandomGenerator},
     * and future ones, to be started from a fixed seed. This is useful for making
     * tests deterministic.
     */
    public static void useTestSeed() {
        useTestSeed = true;
        Collection<RandomGenerator> instances = INSTANCES.get();
        if (instances != null) {
            synchronized (instances) {
                instances.forEach(random -> random.setSeed(TEST_SEED));
                instances.clear();
            }
            INSTANCES.clear();
        }
    }

}
