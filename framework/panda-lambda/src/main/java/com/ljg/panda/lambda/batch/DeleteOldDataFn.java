/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.ljg.panda.lambda.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Function that deletes old data, if applicable, at each batch interval.
 *  删除旧的数据的函数
 *  每一个batch都会运行
 * @param <T> unused
 */
public final class DeleteOldDataFn<T> implements VoidFunction<T> {

    private static final Logger log = LoggerFactory.getLogger(DeleteOldDataFn.class);

    private final Configuration hadoopConf;
    private final String dataDirString;
    private final Pattern dirTimestampPattern;
    private final int maxAgeHours;

    public DeleteOldDataFn(Configuration hadoopConf,
                           String dataDirString,
                           Pattern dirTimestampPattern,
                           int maxAgeHours) {
        this.hadoopConf = hadoopConf;
        this.dataDirString = dataDirString;
        this.dirTimestampPattern = dirTimestampPattern;
        this.maxAgeHours = maxAgeHours;
    }

    @Override
    public void call(T ignored) throws IOException {
        // 获取所有需要删除的文件
        Path dataDirPath = new Path(dataDirString + "/*");
        FileSystem fs = FileSystem.get(dataDirPath.toUri(), hadoopConf);
        FileStatus[] inputPathStatuses = fs.globStatus(dataDirPath);
        if (inputPathStatuses != null) {
            // 需要保留数据的时间点
            long oldestTimeAllowed =
                    System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(maxAgeHours, TimeUnit.HOURS);
            // 找到是文件目录的路径，判断这个文件目录的修改时间是否小于需要保留数据的时间点
            // 如果小于的话则删除该文件目录
            Arrays.stream(inputPathStatuses).filter(FileStatus::isDirectory).map(FileStatus::getPath).
                    filter(subdir -> {
                        Matcher m = dirTimestampPattern.matcher(subdir.getName());
                        return m.find() && Long.parseLong(m.group(1)) < oldestTimeAllowed;
                    }).forEach(subdir -> {
                log.info("Deleting old data at {}", subdir);
                try {
                    fs.delete(subdir, true);
                } catch (IOException e) {
                    log.warn("Unable to delete {}; continuing", subdir, e);
                }
            });
        }
    }

}
