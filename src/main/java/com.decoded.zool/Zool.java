package com.decoded.zool;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import play.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A Zookeeper Client interface
 */
public abstract class Zool {
  private static final Logger.ALogger LOG = Logger.of(Zool.class);

  private final ZookeeperDataFlow zookeeperDataFlow;

  // thread safety
  private AtomicBoolean connected = new AtomicBoolean(false);

  private String host = "localhost";
  private int port = 2181;
  private int timeout = 10000;
  private String serviceMapNode;
  private String gatewayMapNode;

  public Zool(ZookeeperDataFlow zookeeperDataFlow) {
    this.zookeeperDataFlow = zookeeperDataFlow;
  }

  public void setGatewayMapNode(String gatewayMapNode) {
    this.gatewayMapNode = gatewayMapNode;
  }

  public String getGatewayMapNode() {
    return gatewayMapNode;
  }

  public String getServiceMapNode() {
    return serviceMapNode;
  }

  public void setServiceMapNode(String serviceMapNode) {
    this.serviceMapNode = serviceMapNode;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getTimeout() {
    return timeout;
  }

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
      return zookeeperDataFlow.getZk().getChildren(path, false);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted", ex);
    } catch (KeeperException ex) {
      LOG.error("Keeper Exception", ex);
    }
    return Collections.emptyList();
  }

  @Deprecated
  public ZooKeeper getZookeeper() {
    return zookeeperDataFlow.getZk();
  }


  /**
   * Connect to zookeeper.
   */
  public synchronized void connect() {
    if (!connected.get()) {
      connected.set(true);
      this.zookeeperDataFlow.setHost(host)
          .setPort(port)
          .setTimeout(timeout);
      // run the zookeeperDataFlow in its own thread.
      zookeeperDataFlow.connect();
    }
  }

  /**
   * Terminate our zookeeper connection
   */
  public synchronized void disconnect() {
    if (connected.get()) {
      connected.set(false);
      zookeeperDataFlow.terminate();
    }
  }

  public AtomicBoolean isConnected() {
    return connected;
  }

  /**
   * Remove a node
   *
   * @param path the path to remove
   * @return a boolean, true if the path was either removed, or didn't exist to begin with.
   */
  public boolean removeNode(String path) {
    LOG.warn("removeNode: " + path);

    try {
      zookeeperDataFlow.getZk().delete(path, 1);
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
      LOG.info("createNode: " + path + " with data: " + new String(data));
      zookeeperDataFlow.getZk().create(path, data, acls, mode);
      LOG.info("Created " + mode.name() + " node: " + path);
      return true;
    } catch (KeeperException ex) {
      LOG.warn("Keeper exception creating node: ", ex.getMessage());
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
  public void drainStop(ZookeeperDataSink dataSink) {
    zookeeperDataFlow.drainStop(dataSink);
  }

  /**
   * Add a data handler
   *
   * @param dataSink the data sink to plug
   */
  public void drain(ZookeeperDataSink dataSink) {
    zookeeperDataFlow.drain(dataSink);
  }

}
