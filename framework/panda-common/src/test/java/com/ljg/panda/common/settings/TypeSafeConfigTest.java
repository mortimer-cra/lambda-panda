package com.ljg.panda.common.settings;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class TypeSafeConfigTest {

    // 默认是resources下的reference.conf
    // 也可以使用-Dconfig.file=E:\panda.conf指定配置文件
    private static final Config DEFAULT_CONFIG = ConfigFactory.load();

    @Test
    public void testTypeSafeConfig() {
        System.out.println(DEFAULT_CONFIG.getInt("panda.batch.streaming.num-executors"));
    }


}