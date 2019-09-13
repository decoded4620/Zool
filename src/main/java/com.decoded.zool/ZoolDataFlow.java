package com.decoded.zool;

import com.decoded.zool.connection.ZookeeperConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.function.BiConsumer;


/**
 * This object is designed to transport data directly from ZooKeeper signals to your {@link ZoolDataSink} instances.
 */
public interface ZoolDataFlow extends Watcher,
    Runnable,
    ZoolWatcher {
  /**
   * Connect to the host / port set for this data flow.
   */
  void connect();

  /**
   * True if we're connected to a data source
   *
   * @return true if this instance is connected to a zookeeper server.
   */
  boolean isConnected();

  /**
   * Drain this flow into the specified data sync
   *
   * @param dataSink a {@link ZoolDataSink}
   */
  void drain(ZoolDataSink dataSink);

  /**
   * Drain the data at path, into the data handler
   *
   * @param path        the path to drain data from
   * @param watch       watch (or not)
   * @param dataHandler the drain handler
   * @param ctx         the context passed in.
   */
  void drain(String path, boolean watch, BiConsumer<String, byte[]> dataHandler, Object ctx);

  /**
   * Stop the flow of data to the data sink.
   *
   * @param dataSink the data sink currently receiving data from this flow.
   */
  void drainStop(ZoolDataSink dataSink);

  /**
   * Returns the set of node names that are child nodes of the specified path
   *
   * @param path  the specified path
   * @param watch if set true, will watch the path and be notified of changes.
   *
   * @return List of String names.
   */
  List<String> getChildNodesAtPath(String path, boolean watch);

  /**
   * Delete a node at path, with version
   *
   * @param path the path
   *
   * @return true if the delete was successful
   */
  boolean delete(String path);

  /**
   * Create a node
   *
   * @param path the path
   * @param data node data
   * @param acls acl to use for creation
   * @param mode the create mode.
   *
   * @return true if the node was created.
   */
  boolean create(String path, byte[] data, List<ACL> acls, CreateMode mode);

  /**
   * Update the data on a zool node.
   *
   * @param path the path
   * @param data the data
   *
   * @return a boolean if the update was successful.
   */
  boolean update(final String path, final byte[] data);

  /**
   * Get the data from a node.
   *
   * @param path a path
   *
   * @return the byte[] data.
   */
  byte[] get(String path);

  /**
   * Returns true if a node exists
   *
   * @param path the path
   *
   * @return a boolean.
   */
  boolean nodeExists(final String path);

  /**
   * terminates the data flow connection
   */
  void terminate();

  /**
   * Connection details.
   * @return the zookeeper connection
   */
  ZookeeperConnection getZookeeperConnection();
}
