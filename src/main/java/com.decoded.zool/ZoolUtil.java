package com.decoded.zool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


public class ZoolUtil {
  // helper for efficient debug logging
  static void debugIf(Supplier<String> message) {
    if(LOG.isDebugEnabled()) {
      LOG.debug("{" + Thread.currentThread().getName() + "}:" + message.get());
    }
  }
  
  public static final Logger LOG = LoggerFactory.getLogger(ZoolUtil.class);

  /**
   * Create an empty zool node.
   *
   * @param zool Zool instance
   * @param path the node path
   */
  public static void createEmptyPersistentNode(Zool zool, final String path) {
    debugIf(() ->"createEmptyPersistentNode: " + path);
    if (zool.createNode(path, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT)) {
      debugIf(() ->"createEmptyPersistentNode -> node has been created: " + path);
    }
  }

  public static void createEmptyEphemeralNode(Zool zool, final String path) {
    debugIf(() ->": createEmptyEphemeralNode: " + path);
    if (zool.createNode(path, new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL)) {
      debugIf(() ->": createEmptyEphemeralNode -> node has been created: " + path);
    }
  }

  public static void createPersistentNode(Zool zool, final String path, byte[] bytes) {
    debugIf(() ->": createPersistentNode: " + path);
    if (zool.createNode(path, bytes, OPEN_ACL_UNSAFE, PERSISTENT)) {
      debugIf(() ->": createPersistentNode -> node has been created: " + path);
    }
  }

  public static void createEphemeralNode(Zool zool, final String path, byte[] bytes) {
    if (zool.createNode(path, bytes, OPEN_ACL_UNSAFE, EPHEMERAL)) {
      debugIf(() ->":createEphemeralNode -> node has been created: " + path);
    }
  }
}
