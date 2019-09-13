package com.decoded.zool.connection;

import com.google.inject.Inject;


/**
 * Configuration Object for Zookeeper connection
 */
public class ZookeeperConnection {
  private int zkConnectTimeout = -1;
  private String zkHost = null;
  private int zkPort = 0;
  private String zkServiceMapNode = null;

  @Inject
  public ZookeeperConnection() {

  }

  public ZookeeperConnection setZkConnectTimeout(final int zkConnectTimeout) {
    this.zkConnectTimeout = zkConnectTimeout;
    return this;
  }

  public int getZkConnectTimeout() {
    return zkConnectTimeout;
  }

  public ZookeeperConnection setZkHost(final String zkHost) {
    this.zkHost = zkHost;
    return this;
  }

  public String getZkHost() {
    return zkHost;
  }

  public ZookeeperConnection setZkPort(final int zkPort) {
    this.zkPort = zkPort;
    return this;
  }

  public int getZkPort() {
    return zkPort;
  }

  public String getZkHostAddress() {
    return zkHost + ':' + zkPort;
  }
  public ZookeeperConnection setZkServiceMapNode(final String zkServiceMapNode) {
    this.zkServiceMapNode = zkServiceMapNode;
    return this;
  }

  public String getZkServiceMapNode() {
    return zkServiceMapNode;
  }
}
