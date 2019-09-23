package com.ljg.panda.lambda;

import com.ljg.panda.common.lang.PandaShutdownHook;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Hadoop-related utility methods.
 */
public final class HadoopUtils {

  private static final Logger log = LoggerFactory.getLogger(HadoopUtils.class);

  private static final PandaShutdownHook SHUTDOWN_HOOK = new PandaShutdownHook();

  private HadoopUtils() {}

  /**
   * Adds a shutdown hook that tries to call {@link Closeable#close()} on the given argument
   * at JVM shutdown. This integrates with Hadoop's {@link ShutdownHookManager} in order to
   * better interact with Spark's usage of the same.
   *
   * @param closeable thing to close
   */
  public static void closeAtShutdown(Closeable closeable) {
    if (SHUTDOWN_HOOK.addCloseable(closeable)) {
      try {
        // Spark uses SHUTDOWN_HOOK_PRIORITY + 30; this tries to execute earlier
        ShutdownHookManager.get().addShutdownHook(SHUTDOWN_HOOK, FileSystem.SHUTDOWN_HOOK_PRIORITY + 40);
      } catch (IllegalStateException ise) {
        log.warn("Can't close {} at shutdown since shutdown is in progress", closeable);
      }
    }
  }

}
