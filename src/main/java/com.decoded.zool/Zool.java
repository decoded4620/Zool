package com.decoded.zool;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A Zookeeper Client interface
 */
public class Zool {
  private static final Logger LOG = LoggerFactory.getLogger(Zool.class);

  private final ZoolDataFlow zoolDataFlow;

  // thread safety
  private AtomicBoolean connected = new AtomicBoolean(false);

  private String host = "localhost";
  private int port = 2181;
  private int timeout = 10000;
  private String serviceMapNode;
  private String gatewayMapNode;

  public Zool(ZoolDataFlow zoolDataFlow) {
    this.zoolDataFlow = zoolDataFlow;
  }

  public String getGatewayMapNode() {
    return gatewayMapNode;
  }

  /**
   * Set the name of the node pertaining to the gateway map.
   *
   * @param gatewayMapNode the name of a gateway map node.
   */
  public void setGatewayMapNode(String gatewayMapNode) {
    this.gatewayMapNode = gatewayMapNode;
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
  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  /**
   * Zookeeper host port
   *
   * @param port the port.
   */
  public void setPort(int port) {
    this.port = port;
  }

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
   * @param path the path
   * @return the list of nodes.
   */
  public List<String> getChildren(String path) {
    try {
      return zoolDataFlow.getZk().getChildren(path, false);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted", ex);
    } catch (KeeperException ex) {
      LOG.error("Keeper Exception", ex);
    }
    return Collections.emptyList();
  }

  @Deprecated
  public ZooKeeper getZookeeper() {
    return zoolDataFlow.getZk();
  }

  /**
   * Connect to zookeeper.
   */
  public synchronized void connect() {
    if (!connected.get()) {
      connected.set(true);

      this.zoolDataFlow.setHost(host)
          .setPort(port)
          .setTimeout(timeout);

      // run the zoolDataFlow in its own thread.
      zoolDataFlow.connect();
    }
  }

  /**
   * Terminate our zookeeper connection
   */
  public synchronized void disconnect() {
    if (connected.get()) {
      connected.set(false);
      zoolDataFlow.terminate();
    }
  }


  /**
   * Returns the connection status.
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
   * @return a boolean, true if the path was either removed, or didn't exist to begin with.
   */
  public boolean removeNode(String path) {
    try {
      zoolDataFlow.getZk().delete(path, 1);
      return true;
    } catch (KeeperException ex) {
      if (ex.code() == KeeperException.Code.NONODE) {
        return true;
      }
      // if can't create, try to remove our node.
    } catch (InterruptedException ex) {
      LOG.error("Interrupted exception", ex);
    }

    return false;
  }

  /**
   * Create a ZK Node.
   *
   * @param path the path to create the node
   * @param data the data to push to the node
   * @param acls ACLS
   * @param mode the create mode.
   * @return true if the node was created.
   */
  public boolean createNode(String path, byte[] data, ArrayList<ACL> acls, CreateMode mode) {
    try {
      zoolDataFlow.getZk().create(path, data, acls, mode);
      return true;
    } catch (KeeperException ex) {
      LOG.error("Keeper exception creating node: ", ex);
      // if can't create, try to remove our node.
    } catch (InterruptedException ex) {
      LOG.error("Interrupted exception", ex);
    }

    return false;
  }

  /**
   * Plugs a DataSink (e.g. stop draining data)
   *
   * @param dataSink the data sink to plug
   */
  public void drainStop(ZoolDataSink dataSink) {
    zoolDataFlow.drainStop(dataSink);
  }

  /**
   * Add a data handler
   *
   * @param dataSink the data sink to plug
   */
  public void drain(ZoolDataSink dataSink) {
    zoolDataFlow.drain(dataSink);
  }

}
