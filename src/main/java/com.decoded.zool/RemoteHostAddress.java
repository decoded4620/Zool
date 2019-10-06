package com.decoded.zool;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;


/**
 * This object parses a remote host url, a service map node, and a service key into an organized set of data about the
 * host itself. Allowing quick access to things like hostUrl and port, or full url, or the service key of the host.
 */
public class RemoteHostAddress {
  private final String host;
  private final int port;
  private final String serviceKey;
  private final String hostZkNode;
  private final String serviceMapNode;
  private final String hostUrl;

  /**
   * Immutable Object to parse a host address and its related data.
   *
   * @param serviceMapNode the service map node name for your dynamic discovery service
   * @param serviceKey     the service key
   * @param remoteHostUrl  the remote host url.
   */
  public RemoteHostAddress(String serviceMapNode, String serviceKey, String remoteHostUrl) {
    this.hostUrl = remoteHostUrl;
    this.serviceMapNode = serviceMapNode;
    this.serviceKey = serviceKey;
    this.hostZkNode = ZConst.PathSeparator.ZK.join(this.serviceMapNode, this.serviceKey, remoteHostUrl);

    final int portIdx = remoteHostUrl.lastIndexOf(':');
    this.host = portIdx > -1 ? remoteHostUrl.substring(0, portIdx) : remoteHostUrl;
    this.port = portIdx > -1 ? Integer.valueOf(remoteHostUrl.substring(portIdx + 1)) : portIdx;
  }

  /**
   * Returns the host name without port
   *
   * @return String
   */
  public String getHost() {
    return host;
  }

  /**
   * returns the url (with port)
   *
   * @return String
   */
  public String getHostUrl() {
    return hostUrl;
  }

  /**
   * Get the port
   *
   * @return int
   */
  public int getPort() {
    return port;
  }

  /**
   * The service map node name
   *
   * @return String
   */
  public String getServiceMapNode() {
    return serviceMapNode;
  }

  /**
   * The service key for this host
   *
   * @return String
   */
  public String getServiceKey() {
    return serviceKey;
  }

  /**
   * The Host Zookeeper Node Path
   *
   * @return a String, the host zookeeper node
   */
  public String getHostZkNode() {
    return hostZkNode;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RemoteHostAddress that = (RemoteHostAddress) o;

    return new EqualsBuilder().append(port, that.port)
        .append(host, that.host)
        .append(serviceKey, that.serviceKey)
        .append(hostZkNode, that.hostZkNode)
        .append(serviceMapNode, that.serviceMapNode)
        .append(hostUrl, that.hostUrl)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(host)
        .append(port)
        .append(serviceKey)
        .append(hostZkNode)
        .append(serviceMapNode)
        .append(hostUrl)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("host", host)
        .append("port", port)
        .append("serviceKey", serviceKey)
        .append("hostZkNode", hostZkNode)
        .append("serviceMapNode", serviceMapNode)
        .append("hostUrl", hostUrl)
        .toString();
  }
}
