package com.ljg.panda.api.speed;

/**
 * Interface that all Speed Layer in-memory models implement.
 * Speed层中所有存在于内存的模型需要实现的接口
 * 存在于Speed层内存中的Model的抽象
 */
public interface SpeedModel {

    /**
     * 返回已经从Batch Layer中接收到的IDs所占的比例
     * 可以根据这个比例来控制Speed Layer的行为
     *
     * @return fraction of IDs that were expected to be in the model whose value has been
     * loaded from an update
     */
    float getFractionLoaded();

}
