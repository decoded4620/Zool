package com.decoded.zool;

import org.slf4j.Logger;

import java.util.function.Supplier;


public class ZoolLoggingUtil {

  /**
   * Performance aware debug logging without the boilerplate.
   *
   * @param logger an SLF4J logger
   * @param msg    the message to debug.
   */
  public static void debugIf(Logger logger, Supplier<String> msg) {
    if (logger.isDebugEnabled()) {
      logger.debug(msg.get());
    }
  }

  /**
   * Logging Utility to print the current thread if desired along with the message
   *
   * @param logger  the Logger to use
   * @param message the message to log
   */
  public static void infoT(Logger logger, String message) {
    logger.info(message);
  }
}
