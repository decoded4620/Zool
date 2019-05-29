package com.decoded.zool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;


/**
 * Machine Networking specific utilities
 */
public class NetworkUtil {
  private static final Logger LOG = LoggerFactory.getLogger(NetworkUtil.class.getSimpleName());

  private NetworkUtil() {
  }

  /**
   * Returns the canonical machine name.
   * @return the local host name or empty optional.
   */
  public static Optional<String> getMaybeCanonicalLocalhostName() {
    try {
      return Optional.of(InetAddress.getLocalHost().getCanonicalHostName());
    } catch (UnknownHostException e) {
      LOG.error("Could not find local host node name", e);
      return Optional.empty();
    }
  }
}
