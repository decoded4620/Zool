package com.decoded.zool;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


/**
 * DataObject stored on zookeeper for zool announced hosts in each service node created.
 */
public class ZoolAnnouncement {
  @JsonProperty
  public byte[] token = new byte[0];

  @JsonProperty
  public long currentEpochTime = 0L;

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
        return new ZoolAnnouncement();
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
        return new byte[0];
      }
    }

    return new byte[0];
  }

}
