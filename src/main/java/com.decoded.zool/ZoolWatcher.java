package com.decoded.zool;

import org.apache.zookeeper.KeeperException;


/**
 * Other classes use the ZoolDataBridgeImpl by implementing this method
 */
public interface ZoolWatcher extends ZoolDataSink {

  /**
   * The Zool zookeeper session is no longer valid.
   *
   * @param rc       the ZooKeeper reason code
   * @param nodePath the node path
   */
  void onZoolSessionInvalid(KeeperException.Code rc, String nodePath);
}