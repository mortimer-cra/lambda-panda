package com.ljg.panda.common.settings;

import com.typesafe.config.ConfigObject;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *  将typesafe Config转成Properties属性并打印到控制台
 */
public final class ConfigToProperties {
    private ConfigToProperties() {}

    public static void main(String[] args) {
        buildPropertiesLines().forEach(System.out::println);
    }

    static List<String> buildPropertiesLines() {
        ConfigObject config = (ConfigObject) ConfigUtils.getDefault().root().get("panda");
        Map<String,String> keyValueMap = new TreeMap<>();
        add(config, "panda", keyValueMap);
        return keyValueMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.toList());
    }

    private static void add(ConfigObject config, String prefix, Map<String,String> values) {
        config.forEach((key, value) -> {
            String nextPrefix = prefix + "." + key;
            switch (value.valueType()) {
                case OBJECT:
                    add((ConfigObject) value, nextPrefix, values);
                    break;
                case NULL:
                    // do nothing
                    break;
                default:
                    values.put(nextPrefix, String.valueOf(value.unwrapped()));
            }
        });
    }

}
