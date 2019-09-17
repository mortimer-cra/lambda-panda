package com.ljg.panda.common.collection;

import java.util.Comparator;

/**
 *  {@link Pair} 的相关的工具方法
 */
public final class Pairs {

    private Pairs(){}

    /**
     *  对{@link Pair}排序的顺序
     */
    public enum SortOrder {
        ASCENDING,
        DESCENDING,
    }

    /**
     *  根据{@link Pair}的第一个对象进行排序比较
     * @param order 标识是降序排还是升序排
     * @param <C>   需要比较{@link Pair}的第一个对象的类型
     * @param <D>   需要比较{@link Pair}的第二哥对象的类型
     * @return  根据 {@link Pair} 第一个对象进行比较的比较器
     */
    public static <C extends Comparable<C>,D> Comparator<Pair<C,D>> orderByFirst(SortOrder order) {
        Comparator<Pair<C,D>> ordering = Comparator.comparing(Pair::getFirst);
        if (order == SortOrder.DESCENDING) {
            ordering = ordering.reversed();
        }
        return Comparator.nullsLast(ordering);
    }

    /**
     *  根据{@link Pair}的第二个对象进行排序比较
     * @param order 标识是降序排还是升序排
     * @param <C>   需要比较{@link Pair}的第一个对象的类型
     * @param <D>   需要比较{@link Pair}的第二个对象的类型
     * @return  根据 {@link Pair} 第二个对象进行比较的比较器
     */
    public static <C, D extends Comparable<D>> Comparator<Pair<C,D>> orderBySecond(SortOrder order) {
        Comparator<Pair<C,D>> ordering = Comparator.comparing(Pair::getSecond);
        if (order == SortOrder.DESCENDING) {
            ordering = ordering.reversed();
        }
        return Comparator.nullsLast(ordering);
    }

}
