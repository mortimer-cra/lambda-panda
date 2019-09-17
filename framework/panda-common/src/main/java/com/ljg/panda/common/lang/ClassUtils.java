package com.ljg.panda.common.lang;


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 *  {@link Class} 相关的工具方法
 */
public final class ClassUtils {

    private static final Class<?>[] NO_TYPES = new Class<?>[0];
    private static final Object[] NO_ARGS = new Object[0];

    private ClassUtils() {}

    /**
     *  加载一个指定的类
     * @param className 指定的类全名
     * @param <T>   返回的Class的类型
     * @return  返回T类型的Class
     */
    public static <T> Class<T> loadClass(String className) {
        try {
            Class<T> theClass = (Class<T>)forName(className);
            return theClass;
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("No valid " + className + " exists", cnfe);
        }
    }

    /**
     *  加载指定父类的子类
     * @param className 需要加载的类全名
     * @param superClass    父类的Class
     * @param <T>
     * @return
     */
    public static <T> Class<? extends T> loadClass(String className, Class<T> superClass) {
        try {
            return forName(className).asSubclass(superClass);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("No valid " + superClass + " binding exists", cnfe);
        }
    }

    /**
     *  判断指定的类名是否可以加载
     * @param implClassName 类全名
     * @return 如果类可以加载则返回true
     */
    public static boolean classExists(String implClassName) {
        try {
            forName(implClassName);
            return true;
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    private static Class<?> forName(String implClassName) throws ClassNotFoundException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = ClassUtils.class.getClassLoader();
        }
        return Class.forName(implClassName, true, cl);
    }

    /**
     *  根据一个Class<T>类型创建一个T类型的对象
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T loadInstanceOf(Class<T> clazz) {
        return loadInstanceOf(clazz.getName(), clazz);
    }

    /**
     *  利用反射调用指定类的无参构造器进行创建对象
     * @param implClassName 实现类的类名
     * @param superClass    父类或者接口的全名
     * @param <T>
     * @return
     */
    public static <T> T loadInstanceOf(String implClassName, Class<T> superClass) {
        return loadInstanceOf(implClassName, superClass, NO_TYPES, NO_ARGS);
    }

    /**
     *  利用反射给指定类名以及这个类的指定个数参数的构造子创建一个对象
     * @param implClassName 实现类的类名
     * @param superClass    父类或者接口的全名
     * @param constructorTypes  构造器参数的类型
     * @param constructorArgs   传给构造器的参数
     * @param <T>   返回的对象的类型
     * @return  创建好的对象
     */
    public static <T> T loadInstanceOf(String implClassName,
                                       Class<T> superClass,
                                       Class<?>[] constructorTypes,
                                       Object[] constructorArgs) {
        try {
            Class<? extends T> configClass = loadClass(implClassName, superClass);
            Constructor<? extends T> constructor = configClass.getConstructor(constructorTypes);
            return constructor.newInstance(constructorArgs);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("No valid " + superClass + " binding exists", e);
        } catch (InvocationTargetException ite) {
            throw new IllegalStateException("Could not instantiate " + superClass + " due to exception",
                    ite.getCause());
        }
    }

}
