package com.decoded.zool.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;


// support for mongo injected data.
public class ServiceGateway {

  private String serviceKey;

  private List<ServiceHost> hosts;
  // TODO add any addition info required, e.g. auth tokens, etc.


  public List<ServiceHost> getHosts() {
    return hosts;
  }

  @JsonProperty
  public ServiceGateway setServiceKey(final String serviceKey) {
    this.serviceKey = serviceKey;
    return this;
  }

  public String getServiceKey() {
    return serviceKey;
  }

  @JsonProperty
  public ServiceGateway setHosts(final List<ServiceHost> hosts) {
    this.hosts = hosts;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("hosts", hosts).toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ServiceGateway that = (ServiceGateway) o;

    return new EqualsBuilder().append(hosts, that.hosts).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(hosts).toHashCode();
  }
}
