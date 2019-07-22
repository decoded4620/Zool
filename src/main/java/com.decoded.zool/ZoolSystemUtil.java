package com.decoded.zool;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * A Utility for performing some important zool functions that are used in many places. This is a read only object and
 * should not modify the Zool System in any way.
 */
public class ZoolSystemUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolSystemUtil.class);
  private static final String HTTP_PORT_PROP = "http.port";

  /**
   * returns the current http port that the http server is serving on.
   *
   * @param fallbackPort the fallback port.
   *
   * @return the current port, an integer.
   */
  public static int getCurrentPort(int fallbackPort) {
    int currentPort;
    try {
      currentPort = Integer.valueOf(System.getProperties().getProperty(HTTP_PORT_PROP));
    } catch (NumberFormatException ex) {
      currentPort = fallbackPort;
    }

    return currentPort;
  }


  /**
   * Returns the host url
   *
   * @param isProd boolean flag for production vs. dev
   *
   * @return a String
   */
  public static String getLocalHostUrl(boolean isProd) {
    // You must set the environment variable denoted in EnvironmentConstants
    // for both dev and prod PUB DNS to run the container application.
    return Optional.ofNullable(
        System.getenv(isProd ? EnvironmentConstants.PROD_SERVER_DNS : EnvironmentConstants.DEV_SERVER_DNS))
        .orElseGet(() -> {
          LOG.error(
              "Environment Expected to have " + EnvironmentConstants.PROD_SERVER_DNS + ", and " + EnvironmentConstants.DEV_SERVER_DNS + " keys");
          return "";
        });
  }

  /**
   * Returns the DNS based identifier for this machine.
   *
   * @param isProd       boolean
   * @param fallbackPort the fallback port to try
   *
   * @return a String that uniquely identifies this host in the service key cluster on zookeeper.
   */
  public static String getLocalHostUrlAndPort(boolean isProd, int fallbackPort) {
    // You must set the environment variable denoted in EnvironmentConstants
    // for both dev and prod PUB DNS to run the container application.
    return Optional.ofNullable(
        System.getenv(isProd ? EnvironmentConstants.PROD_SERVER_DNS : EnvironmentConstants.DEV_SERVER_DNS))
        .map(value -> value + ':' + getCurrentPort(fallbackPort))
        .orElseGet(() -> {
          LOG.error(
              "Environment Expected to have " + EnvironmentConstants.PROD_SERVER_DNS + ", and " + EnvironmentConstants.DEV_SERVER_DNS + " keys");
          return "";
        });
  }

  /**
   * Retrieve child nodes at the specified path safely.
   *
   * @param zk    zookeeper
   * @param path  the path
   * @param watch watch flag
   *
   * @return list of child node names
   */
  public static List<String> getChildNodesAtPath(final ZooKeeper zk, final String path, final boolean watch) {
    if (zk != null) {
      LOG.info("get child nodes at path: " + path + ", watch " + watch);
      try {
        Stat stat = zk.exists(path, watch);
        if (stat != null) {
          return zk.getChildren(path, watch);
        } else {
          LOG.warn("no node exists at " + path);
        }

      } catch (InterruptedException ex) {
        LOG.error("Interrupted", ex);
      } catch (KeeperException ex) {
        LOG.error("Keeper Exception", ex);
      }
    }
    return Collections.emptyList();
  }
}
