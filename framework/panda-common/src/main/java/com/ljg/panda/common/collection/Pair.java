package com.ljg.panda.common.collection;

import java.io.Serializable;
import java.util.Objects;

/**
 * 将两个对象封装成一对
 *
 * 使用final修饰是表明这个class不能被继承
 *
 * @param <A> 第一个对象的类型
 * @param <B> 第二个对象的类型
 */
public final class Pair<A, B> implements Serializable {
    private final A first;
    private final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first) ^ Objects.hashCode(second);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Pair<A,B> other = (Pair<A,B>) obj;
        return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }

    @Override
    public String toString() {
        return first + "," + second;
    }
}
