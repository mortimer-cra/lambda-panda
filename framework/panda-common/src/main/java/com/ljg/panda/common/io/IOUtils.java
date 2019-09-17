package com.ljg.panda.common.io;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *  I/O-相关的工具方法
 */
public final class IOUtils {
    private static final Logger log = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {}

    /**
     *  删除指定路径的文件或者文件目录。如果是文件目录的话则删除目录下面所有的子目录或者文件
     * @param rootDir   需要删除的文件或者文件目录
     * @throws IOException  删除文件或者文件目录的时候出现错误的时候抛出的异常
     */
    public static void deleteRecursively(Path rootDir) throws IOException {
        if (rootDir == null || !Files.exists(rootDir)) {
            return;
        }

        Files.walkFileTree(rootDir, new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * 列出指定文件目录下所有的文件或者文件目录
     * 如果指定了过滤字符串的话，则列出符合过滤字符串正则的文件
     * @param dir   指定文件目录
     * @param glob  过滤字符串
     * @return
     * @throws IOException
     */
    public static List<Path> listFiles(Path dir, String glob) throws IOException {
        Preconditions.checkArgument(Files.isDirectory(dir), "%s is not a directory", dir);

        List<String> globLevels;
        if (glob == null || glob.isEmpty()) {
            globLevels = Collections.singletonList("*");
        } else {
            globLevels = Arrays.asList(glob.split("/"));
        }
        Preconditions.checkState(!globLevels.isEmpty());

        List<Path> paths = new ArrayList<>();
        paths.add(dir);

        for (String globLevel: globLevels) {
            List<Path> newPaths = new ArrayList<>();
            for (Path existingPath: paths) {
                if (Files.isDirectory(existingPath)) {
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(existingPath, globLevel)) {
                        for (Path path : stream) {
                            if (!path.getFileName().toString().startsWith(".")) { // 剔除以.开头的隐藏文件
                                newPaths.add(path);
                            }
                        }
                    }
                }
            }
            paths = newPaths;
        }
        Collections.sort(paths);
        return paths;
    }

    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                log.warn("Unable to close", e);
            }
        }
    }

    /**
     *  获取一个暂时空闲的端口
     * @return
     * @throws IOException
     */
    public static int chooseFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 0)) {
            return socket.getLocalPort();
        }
    }
}
