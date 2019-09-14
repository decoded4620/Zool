package com.decoded.zool.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;


public class ServiceHost {
  private String hostUrl = "127.0.0.1";
  private int port = 9000;
  private String token = "";
  private boolean isSecure = false;

  // TODO add any addition info required, e.g. auth tokens, etc.

  public ServiceHost() {

  }

  public String getHostUrl() {
    return hostUrl;
  }

  @JsonProperty
  public ServiceHost setHostUrl(final String hostUrl) {
    this.hostUrl = hostUrl;
    return this;
  }

  public int getPort() {
    return port;
  }

  @JsonProperty
  public ServiceHost setPort(final int port) {
    this.port = port;
    return this;
  }

  @JsonProperty
  public ServiceHost setSecure(final boolean secure) {
    isSecure = secure;
    return this;
  }

  public boolean isSecure() {
    return isSecure;
  }

  @JsonProperty
  public ServiceHost setToken(final String token) {
    this.token = token;
    return this;
  }

  public String getToken() {
    return token;
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public String getFullUrl() {
    return hostUrl + (port > -1 ? ":" + port : "");
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("hostUrl", hostUrl)
        .append("port", port)
        .append("token", token)
        .append("isSecure", isSecure)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ServiceHost that = (ServiceHost) o;

    return new EqualsBuilder().append(port, that.port)
        .append(isSecure, that.isSecure)
        .append(hostUrl, that.hostUrl)
        .append(token, that.token)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(hostUrl)
        .append(port)
        .append(token)
        .append(isSecure)
        .toHashCode();
  }
}
