package com.ljg.panda.common.lang;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.IntStream;

/**
 * Utility methods for executing tasks in parallel, possibly returning a result
 * and possibly using a private pool, using Java 8 abstractions.
 *  并行执行Task的一些工具方法
 */
public final class ExecUtils {

    private ExecUtils() {
    }

    /**
     *
     * @param numTasks how many copies of {@code task} to run; all may be run simultaneously,
     *                 and a shared thread pool is used
     * @param task     task to execute; takes an {@code Integer} argument that is the index of
     *                 the task that has been executed in [0, numTasks)
     */
    public static void doInParallel(int numTasks, Consumer<Integer> task) {
        doInParallel(numTasks, numTasks, false, task);
    }

    /**
     *  按照指定的并行度来并行计算一定数量的Task，执行的Task是没有返回值的
     * @param numTasks    how many copies of {@code task} to run （Task的数量）
     * @param parallelism maximum how many tasks to run simultaneously （并行度）
     * @param privatePool whether to create and use a new private thread pool; otherwise
     *                    a shared pool is used. No parallelism or pool is used if {@code parallelism} is 1
     *                    是否要创建一个新的线程池来执行Task
     * @param task        task to execute; takes an {@code Integer} argument that is the index of
     *                    the task that has been executed in [0, numTasks)
     *                    需要执行的Task，这个Task的参数是这个task在所有Task中的index
     */
    public static void doInParallel(int numTasks,
                                    int parallelism,
                                    boolean privatePool,
                                    Consumer<Integer> task) {
        Preconditions.checkArgument(numTasks >= 1);
        Preconditions.checkArgument(parallelism >= 1);
        Objects.requireNonNull(task);
        IntStream range = IntStream.range(0, numTasks);
        IntStream taskIndices = parallelism > 1 ? range.parallel() : range;
        if (parallelism > 1 && privatePool) {
            ForkJoinPool pool = new ForkJoinPool(parallelism);
            try {
                pool.submit(() -> taskIndices.forEach(task::accept)).get();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            } catch (ExecutionException e) {
                throw new IllegalStateException(e.getCause());
            } finally {
                pool.shutdown();
            }
        } else {
            taskIndices.forEach(task::accept);
        }
    }

    /**
     * 按照一定的并行度并行执行task，并且返回task执行完的所有的结果
     * @param numTasks    how many copies of {@code task} to run 需要执行的Task的数量
     * @param parallelism maximum how many tasks to run simultaneously 并行度，就是同时运行task的数量
     * @param privatePool whether to create and use a new private thread pool; otherwise
     *                    a shared pool is used. No parallelism or pool is used if {@code parallelism} is 1
     *                      是否需要创建一个新的私有的线程池，如果不需要的话则使用已经在用的共享线程池
     *                      如果并行度为1的话，则不需要创建线程池
     * @param task        task to execute; takes an {@code Integer} argument that is the index of
     *                    the task that has been executed in [0, numTasks) and returns some value
     *                      需要执行的task，这个task是一个函数，其参数是 {@code Integer} 表示运行的是
     *                      第几个task，task执行完后会返回一个值
     * @param collector   instance used to collect results into the return value 用于收集task运行完返回的值
     * @param <T>         type produced by each task    每一个task运行完后返回值的类型
     * @param <R>         type produced by {@code collector} from task results 经过收集器收集的返回值的类型
     * @return result of collecting individual task results 返回经过收集器收集的每一个task返回的结果的值
     */
    public static <T, R> R collectInParallel(int numTasks,
                                             int parallelism,
                                             boolean privatePool,
                                             Function<Integer, T> task,
                                             Collector<T, ?, R> collector) {
        Preconditions.checkArgument(numTasks >= 1);
        Preconditions.checkArgument(parallelism >= 1);
        Objects.requireNonNull(task);
        Objects.requireNonNull(collector);
        IntStream range = IntStream.range(0, numTasks);
        IntStream taskIndices = parallelism > 1 ? range.parallel() : range;
        if (parallelism > 1 && privatePool) {
            ForkJoinPool pool = new ForkJoinPool(parallelism);
            try {
                return pool.submit(() -> taskIndices.mapToObj(task::apply).collect(collector)).get();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            } catch (ExecutionException e) {
                throw new IllegalStateException(e.getCause());
            } finally {
                pool.shutdown();
            }
        } else {
            return taskIndices.mapToObj(task::apply).collect(collector);
        }
    }

}
