package com.decoded.zool;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;

public interface ZoolDataBridge extends Watcher, AsyncCallback.StatCallback {
  boolean isDead();
}
