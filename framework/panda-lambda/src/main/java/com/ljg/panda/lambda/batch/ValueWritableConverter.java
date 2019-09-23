package com.ljg.panda.lambda.batch;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class that automatically converts a value object to and from a given, corresponding
 * {@link Writable} class. For example, this may convert between {@link String} and
 * {@link org.apache.hadoop.io.Text}.
 *  利用Java的反射机制将java中的类型转换成在Hadoop中可以序列化的类型
 *  比如将java.lang.String类型转换成org.apache.hadoop.io.Text
 */
final class ValueWritableConverter<V> {

    // Hadoop中可以序列化的基本类型与Java基本类型的映射
    private static final Map<Class<?>, Class<?>> WRAPPER_TO_PRIMITIVE;

    static {
        WRAPPER_TO_PRIMITIVE = new HashMap<>();
        WRAPPER_TO_PRIMITIVE.put(Byte.class, byte.class);
        WRAPPER_TO_PRIMITIVE.put(Short.class, short.class);
        WRAPPER_TO_PRIMITIVE.put(Integer.class, int.class);
        WRAPPER_TO_PRIMITIVE.put(Long.class, long.class);
        WRAPPER_TO_PRIMITIVE.put(Float.class, float.class);
        WRAPPER_TO_PRIMITIVE.put(Double.class, double.class);
        WRAPPER_TO_PRIMITIVE.put(Boolean.class, boolean.class);
        WRAPPER_TO_PRIMITIVE.put(Character.class, char.class);
    }

    private final Method fromWritableMethod;
    private final Constructor<? extends Writable> writableConstructor;
    private final Constructor<? extends Writable> writableNoArgConstructor;

    /**
     * @param valueClass    underlying value class, like {@link String} or {@link Integer}
     * @param writableClass must have a method whose return type matches {@code valueClass}
     *                      and whose name starts with "get". Or, in the case of value class {@link String}, the
     *                      {@link #toString()} method will be used if not present. Must also have a constructor
     *                      with a single argument whose type is the value class, and a no-arg constructor.
     */
    <W extends Writable> ValueWritableConverter(Class<V> valueClass, Class<W> writableClass) {
        // 利用反射从writableClass中找到符合下面条件的方法
        // 1：get开头
        // 2：返回类型是valueClass或者是Java基本类型的Class
        Method fromWritableMethod = Arrays.stream(writableClass.getMethods()).
                filter(method -> method.getName().startsWith("get")).
                filter(method -> {
                    Class<?> returnType = method.getReturnType();
                    return returnType.equals(valueClass) || returnType.equals(WRAPPER_TO_PRIMITIVE.get(valueClass));
                }).findFirst().orElse(null);

        // 如果没有找到合适的方法，并且valueClass等于String的Class
        // 那么用writableClass中的toString方法替代
        if (fromWritableMethod == null && String.class.equals(valueClass)) {
            // Special-case String
            try {
                fromWritableMethod = writableClass.getMethod("toString");
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(e);
            }
        }
        Preconditions.checkArgument(fromWritableMethod != null,
                writableClass + " has no method returning " + valueClass);
        this.fromWritableMethod = fromWritableMethod;

        // 利用反射获取writableClass中所有的构造器
        @SuppressWarnings("unchecked")
        Constructor<W>[] constructors = (Constructor<W>[]) writableClass.getConstructors();

        // 从writableClass中找到符合下面条件的构造器
        // 1: 构造器只有一个参数
        // 2: 这个参数的类型等于valueClass的类型，或者等于Java基本类型的Class
        writableConstructor = Arrays.stream(constructors).
                filter(constructor -> constructor.getParameterTypes().length == 1).
                filter(constructor -> {
                    Class<?> paramType = constructor.getParameterTypes()[0];
                    return paramType.equals(valueClass) || paramType.equals(WRAPPER_TO_PRIMITIVE.get(valueClass));
                }).findFirst().orElse(null);
        Objects.requireNonNull(writableConstructor, writableClass + " has no constructor accepting " + valueClass);

        // 从writableClass中找到没有参数的构造器
        writableNoArgConstructor = Arrays.stream(constructors).
                filter(constructor -> constructor.getParameterTypes().length == 0).
                findFirst().orElse(null);
    }

    // 将Writable类型转换成指定的其他类型
    V fromWritable(Writable writable) {
        try {
            @SuppressWarnings("unchecked")
            V value = (V) fromWritableMethod.invoke(writable);
            return value;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    // 将其他类型的对象转换成Writable类型
    // 如果其他类型的对象为空的话，则直接调用Writable的空参构造器，创建一个Writable的对象
    Writable toWritable(V value) {
        try {
            if (value == null) {
                return writableNoArgConstructor.newInstance();
            } else {
                return writableConstructor.newInstance(value);
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

}
