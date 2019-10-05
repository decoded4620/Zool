package com.decoded.zool;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.decoded.zool.ZoolLoggingUtil.debugIf;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


/**
 * A Mechanism for writing data to Zookeeper through {@link Zool}
 */
public class ZoolWriter {

  public static final Logger LOG = LoggerFactory.getLogger(ZoolWriter.class);

  private Zool zool;

  @Inject
  public ZoolWriter(Zool zool) {
    this.zool = zool;
  }

  /**
   * Create an empty zool node.
   *
   * @param zool Zool instance
   * @param path the node path
   */
  static boolean createEmptyPersistentNode(Zool zool, final String path) {
    debugIf(LOG, () -> "createEmptyPersistentNode: " + path);
    if (zool.createNode(path, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT)) {
      debugIf(LOG, () -> "createEmptyPersistentNode -> node has been created: " + path);
      return true;
    }

    return false;
  }

  /**
   * Creates an empty ephemeral node on zookeeper
   *
   * @param zool the {@link Zool} instance
   * @param path the string path instance.
   */
  static boolean createEmptyEphemeralNode(Zool zool, final String path) {
    ZoolLoggingUtil.debugIf(LOG, () -> ": createEmptyEphemeralNode: " + path);
    if (zool.createNode(path, new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL)) {
      debugIf(LOG, () -> ": createEmptyEphemeralNode -> node has been created: " + path);
      return true;
    }

    return false;
  }

  /**
   * Creates a persistent node on Zookeeper instance under the path specified. Synchronous
   *
   * @param zool  the {@link Zool} instance
   * @param path  the path
   * @param bytes the data to store at the path.
   */
  static boolean createPersistentNode(Zool zool, final String path, byte[] bytes) {
    debugIf(LOG, () -> ": createPersistentNode: " + path);
    if (zool.createNode(path, bytes, OPEN_ACL_UNSAFE, PERSISTENT)) {
      debugIf(LOG, () -> ": createPersistentNode -> node has been created: " + path);
      return true;
    }

    return false;
  }

  /**
   * Creates an ephemeral node on Zookeeper, Synchronous
   *
   * @param zool  the {@link Zool} instance
   * @param path  the path
   * @param bytes the data to put on the node.
   */
  static boolean createEphemeralNode(Zool zool, final String path, byte[] bytes) {
    debugIf(LOG, () -> "createEphemeralNode: " + path + " with " + bytes.length + " bytes");
    if (zool.createNode(path, bytes, OPEN_ACL_UNSAFE, EPHEMERAL)) {
      debugIf(LOG, () -> ":createEphemeralNode -> node has been created: " + path);
      return true;
    }

    return false;
  }

  /**
   * returns the {@link Zool} instance associated with this writer.
   *
   * @return Zool
   */
  public Zool getZool() {
    return zool;
  }

  /**
   * Remove a node.
   *
   * @param path the path to remove
   *
   * @return a boolean, true if the path was either removed, or didn't exist to begin with.
   */
  public boolean removeNode(String path) {
    return zool.removeNode(path);
  }

  /**
   * Returns true if a node exists at the specified path.
   *
   * @param path the node path
   *
   * @return a boolean
   */
  public boolean nodeExists(String path) {
    return zool.nodeExists(path);
  }

  /**
   * Updates the data on a zool node, may cause broadcasts.
   *
   * @param path  the string path
   * @param bytes the bytes to update
   *
   * @return a boolean <code>true</code> if the node was updated.
   */
  public boolean updateNode(String path, byte[] bytes) {
    return zool.updateNode(path, bytes);
  }


  /**
   * Create (or updates) a node with the specified data.
   *
   * @param path  the node path
   * @param bytes the data bytes
   *
   * @return boolean <code>true</code> if the node was created or updated.
   */
  public boolean createOrUpdateNode(String path, byte[] bytes) {
    if (zool.nodeExists(path)) {
      return zool.updateNode(path, bytes);
    } else {
      return createPersistentNode(path, bytes);
    }
  }

  /**
   * Creates or updates an ephemeral node with the specified data.
   *
   * @param path  the node path.
   * @param bytes the data bytes.
   *
   * @return boolean <code>true</code> if the node was created or updated.
   */
  public boolean createOrUpdateEphemeralNode(String path, byte[] bytes) {
    if (zool.nodeExists(path)) {
      return zool.updateNode(path, bytes);
    } else {
      return createEphemeralNode(path, bytes);
    }
  }

  /**
   * Create a persistent node
   *
   * @param path  the node path
   * @param bytes the bytes
   *
   * @return boolean <code>true</code> if the node was created.
   */
  public boolean createPersistentNode(String path, byte[] bytes) {
    debugIf(LOG, () -> "createPersistentNode: " + path + " with " + bytes.length + " bytes");
    return createPersistentNode(zool, path, bytes);
  }

  /**
   * Create a persistent node
   *
   * @param path the path to create an empty node at.
   *
   * @return a boolean <code>true</code> if the node was created.
   */
  public boolean createPersistentNode(String path) {
    debugIf(LOG, () -> "createPersistentNode: " + path);
    return createEmptyPersistentNode(this.zool, path);
  }

  /**
   * Create an empty ephemeral node.
   *
   * @param path the path to create it at
   *
   * @return a boolean <code>true</code> if the node was created.
   */
  public boolean createEphemeralNode(String path) {
    debugIf(LOG, () -> "createEphemeralNode: " + path);
    return createEmptyEphemeralNode(this.zool, path);
  }

  public boolean createEphemeralNode(String path, byte[] data) {
    debugIf(LOG, () -> "createEphemeralNode: " + path + " with " + data.length + " bytes");
    return createEphemeralNode(this.zool, path, data);
  }
}
