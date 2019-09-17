package com.ljg.panda.common.collection;

import java.io.Closeable;
import java.util.Iterator;

/**
 *  可以关闭的Iterator
 * @param <T> Iterator中元素的类型
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    // 重写接口Closeable中的close方法
    // 只是把Closeable中的close方法声明的异常去掉了，
    // 因为实现接口CloseableIterator的close方法不需要抛 IOException
    @Override
    void close();
}
