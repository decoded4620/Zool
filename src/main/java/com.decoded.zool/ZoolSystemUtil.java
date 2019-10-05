package com.decoded.zool;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.System.getenv;


/**
 * A Utility for performing some important zool functions that are used in many places. This is a read only object and
 * should not modify the Zool System in any way.
 */
public class ZoolSystemUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolSystemUtil.class);
  /**
   * JVM System Properties governing secure and insecure ports
   */
  private static final String HTTP_PORT_PROP = "http.port";
  private static final String HTTPS_PORT_PROP = "https.port";
  private static final int TOKEN_EXPIRY_TIME = 1000 * 60 * 5;

  // TODO support turning off HTTP?
  /**
   * Host http port, Set based on the JVM System Properties at startup
   */
  private static final int HOST_PORT;
  /**
   * Host https port, Set based on the JVM System Properties at startup
   */
  private static final int SECURE_HOST_PORT;

  private static final boolean IS_SECURE;

  static {
    Object portValue = System.getProperties().getProperty(HTTP_PORT_PROP);
    int hostPortTry;
    if (portValue != null) {
      try {
        hostPortTry = Integer.valueOf(String.valueOf(portValue));
      } catch (NumberFormatException ex) {
        hostPortTry = -1;
      }

    } else {
      hostPortTry = 9000;
    }

    HOST_PORT = hostPortTry;

    Object securePortValue = System.getProperties().getProperty(HTTPS_PORT_PROP);
    int secureHostPortTry;
    if (securePortValue != null) {
      secureHostPortTry = Integer.valueOf(String.valueOf(securePortValue));
    } else {
      secureHostPortTry = -1;
    }

    SECURE_HOST_PORT = secureHostPortTry;

    // set this true if there is a specified secure port on the system properties.
    IS_SECURE = SECURE_HOST_PORT != -1;
  }

  /**
   * returns the current http port that the http server is serving on.
   *
   * @return the current port, an integer.
   */
  public static int getCurrentPort() {
    return getCurrentPort(false);
  }

  /**
   * Get the current secure SSL Port.
   *
   * @return the port
   */
  public static int getCurrentSecurePort() {
    return getCurrentPort(true);
  }

  /**
   * Gets a port (either http or https) from system properties to build our local host url
   *
   * @param isHttps true if secure
   *
   * @return a port number
   */
  private static int getCurrentPort(boolean isHttps) {
    return isHttps ? SECURE_HOST_PORT : HOST_PORT;
  }

  /**
   * Returns true if the host is running on a secure port.
   * @return boolean, true if we're a secure host.
   */
  public static boolean isSecure() {
    return IS_SECURE;
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
        getenv(isProd ? EnvironmentConstants.PROD_SERVER_DNS : EnvironmentConstants.DEV_SERVER_DNS)).orElseGet(() -> {
      LOG.error(
          "Environment Expected to have " + EnvironmentConstants.PROD_SERVER_DNS + ", and " + EnvironmentConstants.DEV_SERVER_DNS + " keys");
      return "";
    });
  }

  /**
   * Generates an instance token for a host uri, and base64 encodes it. A new token value is generated each minute.
   *
   * @param hostUri the host uri, e.g. <code>"localhost:9000"</code>
   *
   * @return a byte[] for the new token generated.
   */
  public static byte[] getInstanceToken(String hostUri) {
    // generates a new token every 5 minutes
    return Base64.getEncoder().encode((hostUri + "::" + System.currentTimeMillis() / TOKEN_EXPIRY_TIME).getBytes());
  }

  /**
   * Returns the DNS based identifier for this machine.
   *
   * @param isProd   boolean
   * @param isSecure true if this is a secure host.
   * @param zoolConfig the current configuration being used with our zool connection
   * @return a String that uniquely identifies this host in the service key cluster on zookeeper.
   */
  public static String getLocalHostUrlAndPort(boolean isProd, boolean isSecure, ZoolConfig zoolConfig) {
    // You must set the environment variable denoted in EnvironmentConstants
    // for both dev and prod PUB DNS to run the container application.
    return Optional.ofNullable(
        getenv(isProd ? EnvironmentConstants.PROD_SERVER_DNS : EnvironmentConstants.DEV_SERVER_DNS))
        .map(value -> {
          if(zoolConfig.usengrok) {
            if (value.endsWith("ngrok.io")) {
              // special case ONLY for ngrok (because it means we're hiding the port information
              return value;
            }
          }

          return value + ":" + getCurrentPort(isSecure);
        })
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
      ZoolLoggingUtil.debugIf(LOG, () -> "get child nodes at path: " + path + ", watch " + watch);
      try {
        Stat stat = zk.exists(path, watch);
        if (stat != null) {
          return zk.getChildren(path, watch);
        } else {
          LOG.warn("no node exists at " + path);
        }

      } catch (KeeperException.ConnectionLossException ex) {
        LOG.warn("Zookeeper Disconnected on path: {}, {}", path, ex.getMessage());
      } catch (KeeperException ex) {
        LOG.error("Keeper Exception", ex);
      } catch (InterruptedException ex) {
        LOG.error("Interrupted", ex);
      }
    }
    return Collections.emptyList();
  }
}
