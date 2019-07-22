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

  private String zookeeperHost = "localhost";
  private int port = 2181;
  private int timeout = 10000;
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
   * The zookeeper host url
   *
   * @return the url.
   */
  public String getZookeeperHost() {
    return zookeeperHost;
  }

  /**
   * Set the Zookeeper host url
   *
   * @param zookeeperHost the host for zookeeper, e.g. 127.0.0.1
   */
  public void setZookeeperHost(String zookeeperHost) {
    this.zookeeperHost = zookeeperHost;
  }

  /**
   * Get the port
   *
   * @return an int
   */
  public int getPort() {
    return port;
  }

  /**
   * Zookeeper zookeeper Host port
   *
   * @param port the port.
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * The zookeeper timeout value
   *
   * @return an int
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * Timeout for zk calls.
   *
   * @param timeout the timeout.
   */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
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
  public synchronized void connect() {
    if (!connected.get()) {
      infoIf(LOG, () -> "connecting to ZooKeeper on " + zookeeperHost + ":" + port + ", timeout: " + timeout);
      connected.set(true);

      // set the zookeeperHost port and timeout for the main zool data flow.
      this.zoolDataFlow.setHost(zookeeperHost).setPort(port).setTimeout(timeout);

      // run the zoolDataFlow in its own thread.
      zoolDataFlow.connect();
    }
  }

  /**
   * Terminate our zookeeper connection
   */
  public synchronized void disconnect() {
    if (connected.get()) {
      infoIf(LOG, () -> "disconnecting from ZooKeeper on " + zookeeperHost + ":" + port);
      connected.set(false);
      zoolDataFlow.terminate();
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
    infoIf(LOG, () -> "Deleting ZK Node " + zookeeperHost + ":" + port + " - " + path);
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
    LOG.info(Thread.currentThread()
        .getName() + ":Creating ZK Node " + zookeeperHost + ":" + port + " - " + path + " with " + data.length + " " + "bytes [" + mode
        .name() + "]");
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
   * Plugs a {@link ZoolDataSink} (e.g. stop draining data)
   *
   * @param dataSink the data sink to plug
   */
  public void drainStop(ZoolDataSink dataSink) {
    LOG.info("Drain stop on sink (node): " + dataSink.getZNode());
    zoolDataFlow.drainStop(dataSink);
  }

  /**
   * Add a data handler
   *
   * @param dataSink the data sink to drain into
   */
  public void drain(ZoolDataSink dataSink) {
    LOG.info("Drain to sink (node): " + dataSink.getZNode());
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
    LOG.info("Drain to handler (on node): " + path + " watch: " + watch);
    zoolDataFlow.drain(path, watch, dataHandler, ctx);
  }
}
