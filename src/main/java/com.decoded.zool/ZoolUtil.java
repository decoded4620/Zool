package com.decoded.zool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


public class ZoolUtil {
  public static final Logger LOG = LoggerFactory.getLogger(ZoolUtil.class);

  /**
   * Create an empty zool node.
   *
   * @param zool Zool instance
   * @param path the node path
   */
  public static void createEmptyPersistentNode(Zool zool, final String path) {
    if (zool.createNode(path, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT)) {
      LOG.info(Thread.currentThread().getName() + ": createEmptyPersistentNode -> node has been created: " + path);
    }
  }

  public static void createEmptyEphemeralNode(Zool zool, final String path) {
    if (zool.createNode(path, new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL)) {
      LOG.info(Thread.currentThread().getName() + ": createEmptyEphemeralNode -> node has been created: " + path);
    }
  }

  public static void createPersistentNode(Zool zool, final String path, byte[] bytes) {
    if (zool.createNode(path, bytes, OPEN_ACL_UNSAFE, PERSISTENT)) {
      LOG.info(Thread.currentThread().getName() + ": createPersistentNode -> node has been created: " + path);
    }
  }

  public static void createEphemeralNode(Zool zool, final String path, byte[] bytes) {
    if (zool.createNode(path, bytes, OPEN_ACL_UNSAFE, EPHEMERAL)) {
      LOG.info(Thread.currentThread().getName() + ": createEphemeralNode -> node has been created: " + path);
    }
  }
}
