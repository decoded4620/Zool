package com.decoded.zool;

import org.apache.zookeeper.KeeperException;


/**
   * Other classes use the ZookeeperDataSinkBridge by implementing this method
   */
  public interface ZookeeperEventListener {
    /**
     * The existence status of the node has changed.
     *
     * @param data a byte[]
     * @param nodePath the path of the node we're listening to
     */
    void onZNodeData(byte data[], String nodePath);

  /**
   * When no data exists.
   * @param nodePath the path
   */
  void onZNodeDataNotExists(String nodePath);

    /**
     * The ZooKeeper session is no longer valid.
     *
     * @param rc the ZooKeeper reason code
     * @param nodePath the node path
     */
    void onZookeeperSessionInvalid(KeeperException.Code rc, String nodePath);
  }