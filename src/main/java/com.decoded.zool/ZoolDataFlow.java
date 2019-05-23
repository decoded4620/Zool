package com.decoded.zool;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


/**
 * This object is designed to transport data directly from ZooKeeper signals
 * to your {@link ZoolDataSink} instances.
 */
public interface ZoolDataFlow extends Watcher, Runnable, ZoolWatcher {
  /**
   * Set the Zookeeper host, e.g. <code>localhost</code>
   * @param host the host string
   * @return this data flow
   */
  ZoolDataFlowImpl setHost(String host);

  /**
   * Set the Zookeeper port, e.g. <code>2181</code>
   * @param port the port
   * @return this data flow
   */
  ZoolDataFlowImpl setPort(int port);

  /**
   * The amount of time to wait on zookeeper reponses before timing out.
   * @param timeout a int
   * @return this data flow.
   */
  ZoolDataFlowImpl setTimeout(int timeout);

  /**
   * Connect to the host / port set for this data flow.
   */
  void connect();

  /**
   * True if we're connected to a data source
   * @return true if this instance is connected to a zookeeper server.
   */
  boolean isConnected();

  /**
   * Drain this flow into the specified data sync
   * @param dataSink a {@link ZoolDataSink}
   */
  void drain(ZoolDataSink dataSink);

  /**
   * Stop the flow of data to the data sink.
   * @param dataSink the data sink currently receiving data from this flow.
   */
  void drainStop(ZoolDataSink dataSink);

  /**
   * Return the zookeeper instance
   *
   * @return ZooKeeper
   * @deprecated This is experimental to expose the raw api, and will go away.
   */
  @Deprecated
  ZooKeeper getZk();

  /**
   * terminates the data flow connection
   */
  void terminate();
}
