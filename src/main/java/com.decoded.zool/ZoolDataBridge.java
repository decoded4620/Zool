package com.decoded.zool;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;


/**
 * This represents a connection to a {@link org.apache.zookeeper.ZooKeeper} instance. This extends the Watcher, and the
 * State Callback interfaces, and includes an isDead check for disconnect.
 */
public interface ZoolDataBridge extends Watcher,
    AsyncCallback.StatCallback {
  /**
   * Returns true if the databridge is dead and no longer connected to zookeeper.
   *
   * @return a boolean.
   */
  boolean isDead();

  /**
   * Returns the main node path for this bridge.
   * @return a String
   */
  String getNodePath();

  /**
   * Destroys the bridge (disconnection)
   */
  void burn();
}
