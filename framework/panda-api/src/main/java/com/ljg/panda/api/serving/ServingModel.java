package com.ljg.panda.api.serving;

/**
 * Interface that all Serving Layer in-memory models implement.
 * Serving层存在于内存中的模型必须实现的接口
 */
public interface ServingModel {

    /**
     * 返回已经从Batch Layer中接收到的IDs所占的比例
     * 可以根据这个比例来控制Server Layer的行为
     *
     * @return fraction of IDs that were expected to be in the model whose value has been
     * loaded from an update
     */
    float getFractionLoaded();

}
