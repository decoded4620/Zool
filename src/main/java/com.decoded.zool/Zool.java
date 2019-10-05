package com.decoded.zool;

import com.google.inject.Inject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.decoded.zool.ZoolLoggingUtil.infoIf;


/**
 * Zool is a Zookeeper Interface Client for Microservice Configuration and Coordination Zool can act as both the
 * announcer hub, or the client zookeeperHost which announces itself among the other hosts on the network.
 */
public class Zool {
  private static final Logger LOG = LoggerFactory.getLogger(Zool.class);

  private final ZoolDataFlow zoolDataFlow;

  // thread safety
  private AtomicBoolean connected = new AtomicBoolean(false);

  private String serviceMapNode;

  /**
   * Constructor
   *
   * @param zoolDataFlow a {@link ZoolDataFlow}
   */
  @Inject
  public Zool(ZoolDataFlow zoolDataFlow) {
    this.zoolDataFlow = zoolDataFlow;
  }

  public String getServiceMapNode() {
    return serviceMapNode;
  }

  /**
   * Set the name of the node pertaining to the service map
   *
   * @param serviceMapNode the name of the node
   */
  public void setServiceMapNode(String serviceMapNode) {
    this.serviceMapNode = serviceMapNode;
  }

  /**
   * Get Children nodes at a specific path.
   *
   * @param path  the path
   * @param watch to continue to watch the node at "path" so that new additional children will cause notification
   *
   * @return the list of nodes.
   */
  public List<String> getChildren(String path, boolean watch) {
    return zoolDataFlow.getChildNodesAtPath(path, watch);
  }

  /**
   * Connect to zookeeper, using the zookeeper Host and port.
   */
  public void connect() {
    if (!connected.get()) {
      infoIf(LOG, () -> "connecting to ZooKeeper on " + zoolDataFlow.getZookeeperConnection().getZkHostAddress() + ", timeout: " + zoolDataFlow.getZookeeperConnection().getZkConnectTimeout());

      // run the zoolDataFlow in its own thread.
      zoolDataFlow.connect();

      connected.set(true);
    }
  }

  /**
   * Terminate our zookeeper connection
   */
  public void disconnect() {
    if (connected.get()) {
      infoIf(LOG, () -> "disconnecting from ZooKeeper on " + zoolDataFlow.getZookeeperConnection().getZkHostAddress());
      zoolDataFlow.terminate();
      connected.set(false);
    } else {
      LOG.error("Zool is not connected!");
    }
  }

  /**
   * Returns the connection flag. <code>true</code> if we're connected, <code>false</code> otherwise
   *
   * @return a boolean.
   */
  public boolean isConnected() {
    return connected.get();
  }

  /**
   * Remove a node
   *
   * @param path the path to remove
   *
   * @return a boolean, true if the path was either removed, or didn't exist to begin with.
   */
  public boolean removeNode(String path) {
    infoIf(LOG, () -> "Deleting ZK Node " + zoolDataFlow.getZookeeperConnection().getZkHostAddress() + " - " + path);
    return zoolDataFlow.delete(path);
  }

  /**
   * Create a {@link ZooKeeper} Node on the remote zookeeper quarum
   *
   * @param path the path to create the node
   * @param data the data to push to the node
   * @param acls The ACL to create the node against
   * @param mode the create mode.
   *
   * @return true if the node was created.
   */
  public boolean createNode(String path, byte[] data, ArrayList<ACL> acls, CreateMode mode) {
    infoIf(LOG, () -> Thread.currentThread().getName() + ":Creating ZK Node "
        + zoolDataFlow.getZookeeperConnection().getZkHostAddress()
        + " - " + path + " with " + data.length
        + " " + "bytes [" + mode.name() + "]");

    return zoolDataFlow.create(path, data, acls, mode);
  }

  /**
   * Updates the node through the dataflow.
   *
   * @param path The path to update
   * @param data the data to update with
   *
   * @return true if the update occurred.
   */
  public boolean updateNode(String path, byte[] data) {
    return zoolDataFlow.update(path, data);
  }

  /**
   * True if the node at path already exists
   *
   * @param path the path
   *
   * @return a boolean
   */
  public boolean nodeExists(String path) {
    return zoolDataFlow.nodeExists(path);
  }

  /**
   * Get the data from a node directly.
   *
   * @param node a node.
   *
   * @return the byte[] data.
   */
  public byte[] getData(String node) {
    return zoolDataFlow.get(node);
  }

  /**
   * Plugs a {@link ZoolDataSink} (e.g. disconnect draining data)
   *
   * @param dataSink the data sink to plug
   */
  public void drainStop(ZoolDataSink dataSink) {
    LOG.debug("Drain disconnect on sink (node): " + dataSink.getZNode());
    zoolDataFlow.drainStop(dataSink);
  }

  /**
   * Add a data handler
   *
   * @param dataSink the data sink to drain into
   */
  public void drain(ZoolDataSink dataSink) {
    LOG.debug("Drain to sink (node): " + dataSink.getZNode());
    zoolDataFlow.drain(dataSink);
  }

  /**
   * Dump data from the path to the data handler.
   *
   * @param path        the path to drain data from
   * @param watch       the watch flag
   * @param dataHandler the data handler.
   * @param ctx         the context (optional)
   */
  public void drain(String path, boolean watch, BiConsumer<String, byte[]> dataHandler, Object ctx) {
    LOG.debug("Drain to handler (on node): " + path + " watch: " + watch);
    zoolDataFlow.drain(path, watch, dataHandler, ctx);
  }
}
