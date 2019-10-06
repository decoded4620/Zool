package com.decoded.zool;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * DataObject stored on zookeeper for zool announced hosts in each service node created.
 */
@JsonIgnoreProperties("_id")
public class ZoolAnnouncement {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolAnnouncement.class);
  @JsonProperty
  public byte[] token = new byte[0];

  @JsonProperty
  public long currentEpochTime = 0L;

  /**
   * True if this is a secure host
   */
  @JsonProperty
  public boolean securehost = false;

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("token", token)
        .append("currentEpochTime", currentEpochTime)
        .append("securehost", securehost)
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

    final ZoolAnnouncement that = (ZoolAnnouncement) o;

    return new EqualsBuilder().append(currentEpochTime, that.currentEpochTime)
        .append(securehost, that.securehost)
        .append(token, that.token)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(token).append(currentEpochTime).append(securehost).toHashCode();
  }

  /**
   * Deserialize a byte[] of JSON data to a {@link ZoolAnnouncement}
   *
   * @param jsonBytes the bytes
   *
   * @return a {@link ZoolAnnouncement}
   */
  public static ZoolAnnouncement deserialize(byte[] jsonBytes) {
    if (jsonBytes.length > 0) {
      try {
        return new ObjectMapper().readValue(jsonBytes, ZoolAnnouncement.class);
      } catch (IOException ex) {
        LOG.warn("Could not deserialize zool announcement", ex);
      }
    }

    return new ZoolAnnouncement();
  }

  /**
   * Serialize a {@link ZoolAnnouncement} into a byte[] of json data
   *
   * @param announcement the {@link ZoolAnnouncement}
   *
   * @return a byte[]
   */
  public static byte[] serialize(ZoolAnnouncement announcement) {
    if (announcement != null) {
      try {
        return new ObjectMapper().writeValueAsBytes(announcement);
      } catch (IOException ex) {
        LOG.warn("Could not serialize zool announcement", ex);
      }
    }

    return new byte[0];
  }
}
