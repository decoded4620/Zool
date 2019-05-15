package com.decoded.zool;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public interface ZoolDataFlow extends Watcher, Runnable, ZoolWatcher {
  ZoolDataFlowImpl setHost(String host);

  ZoolDataFlowImpl setPort(int port);

  ZoolDataFlowImpl setTimeout(int timeout);

  void connect();

  void drain(ZoolDataSink dataSink);

  void drainStop(ZoolDataSink dataSink);

  ZooKeeper getZk();

  void terminate();
}
