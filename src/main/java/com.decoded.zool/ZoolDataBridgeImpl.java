package com.decoded.zool;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.decoded.zool.ZoolLoggingUtil.debugIf;
import static com.decoded.zool.ZoolLoggingUtil.infoIf;


/**
 * Watcher and Stat Callback for Zookeeper DataSink objects.
 */
public class ZoolDataBridgeImpl implements ZoolDataBridge {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolDataBridgeImpl.class);
  private final boolean readChildren;
  private ZooKeeper zk;
  private String nodePath;
  private boolean dead;
  private ZoolWatcher zoolWatcher;
  private byte[] prevData;


  /**
   * Constructor
   *
   * @param zk           {@link ZooKeeper} instance
   * @param nodePath     the path to a node to monitor on the server
   * @param zoolWatcher  the listener to handle events.
   * @param readChildren to read or not to read children.
   */
  public ZoolDataBridgeImpl(ZooKeeper zk, String nodePath, ZoolWatcher zoolWatcher, boolean readChildren) {
    this.zk = zk;
    this.nodePath = nodePath;
    this.zoolWatcher = zoolWatcher;
    this.readChildren = readChildren;
    // Get things started by checking if the node onZNodeData. We are going
    // to be completely event driven
    signal(Event.EventType.NodeChildrenChanged);
  }

  @Override
  public String getNodePath() {
    return nodePath;
  }

  @Override
  public void process(WatchedEvent event) {
    final String path = event.getPath();
    debugIf(LOG, () -> "Processing event " + event.getType());
    if (event.getType() == Event.EventType.None) {
      if (event.getState().equals(Event.KeeperState.Expired)) {
        die(Code.SESSIONEXPIRED);
      }
    } else {
      // Something has changed on the node, let's find out
      // only handle our own node.
      if (path != null && path.equals(nodePath)) {
        infoIf(LOG, () -> "Processing Event " + event.getType() + " at: " + path);
        signal(event.getType());
      }
    }
  }

  @Override
  public void processResult(int rc, String path, Object ctx, Stat stat) {
    final Code code = Code.get(rc);
    debugIf(LOG, () -> "Process Result: " + path + ", Stat: " + stat);
    switch (code) {
      case OK:
      case NONODE:
        // this is ok will be passed on
        break;
      case SESSIONEXPIRED:
      case NOAUTH:
        die(code);
        return;
      default:
        // Retry errors
        signal(Event.EventType.None);
        return;
    }

    if (null != path && path.equals(nodePath)) {
      status(code);
    }
  }

  private boolean isProcessableEvent(Event.EventType eventType) {
    boolean isProcessable = eventType.equals(Event.EventType.NodeCreated) || eventType.equals(
        Event.EventType.NodeDeleted) || eventType.equals(Event.EventType.None) || eventType.equals(
        Event.EventType.NodeChildrenChanged) || eventType.equals(Event.EventType.NodeDataChanged);

    infoIf(LOG, () -> "Event: " + eventType + (isProcessable ? " is" : " is not") + " processable");
    return isProcessable;
  }

  /**
   * Singles for a zookeeper state check for our node path
   */
  private void signal(Event.EventType eventType) {
    if (zk != null) {
      if (zk.getState() == ZooKeeper.States.CONNECTING) {
        // wait for a connection before starting up if we're supposed to announce
        while (zk.getState() == ZooKeeper.States.CONNECTING) {
          try {
            infoIf(LOG, () -> "Waiting for zookeeper to connect on thread: " + Thread.currentThread().getName());
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            die(Code.SESSIONEXPIRED);
            return;
          }
        }
      }

      if (zk.getState()
          .isConnected() || (zk.getState() != ZooKeeper.States.NOT_CONNECTED && zk.getState() != ZooKeeper.States.AUTH_FAILED)) {
        if (isProcessableEvent(eventType)) {
          infoIf(LOG,
              () -> "signal on processable event " + eventType.name() + ": " + nodePath + " readChildren: " + readChildren);
          zk.exists(nodePath, true, this, null);

          if (readChildren) {
            debugIf(LOG, () -> "Event: " + eventType + ", reading cluster hosts on: " + nodePath);
            try {
              List<String> children = zk.getChildren(nodePath, true);

              Optional.ofNullable(zoolWatcher).ifPresent(zwatcher -> {
                if (children.isEmpty()) {
                  zwatcher.onNoChildren(nodePath);
                } else {
                  zwatcher.onChildren(nodePath, children);
                }
              });
            } catch (KeeperException.ConnectionLossException ex) {
              infoIf(LOG, () -> "Zookeeper connection is unavailable [" + ex.getMessage() + "], waiting for connection...");
            } catch (KeeperException ex) {
              LOG.error("Could not get children due to zookeeper error", ex);
            } catch (InterruptedException ex) {
              LOG.error("Interrupted while fetching children", ex);
            }
          } else {
            infoIf(LOG, () -> "Not reading children of " + nodePath);
          }
        } else {
          LOG.warn("Ignoring zookeeper event: " + eventType);
        }
      } else {
        LOG.warn("Zookeeper is not connected");
      }
    } else {
      LOG.error("Zool is improperly configured, no zookeeper instance is available");
    }
  }


  /**
   * Safely returns child nodes at a specific path.
   *
   * @param path  the path to get children from.
   * @param watch boolean
   *
   * @return a list of Strings.
   *
   * @deprecated use {@link ZoolSystemUtil}
   */
  public List<String> getChildNodesAtPath(final String path, final boolean watch) {
    return ZoolSystemUtil.getChildNodesAtPath(zk, path, watch);
  }

  /**
   * status of our node path / connection
   *
   * @param statusCode a status code.
   */
  private void status(Code statusCode) {
    byte[] newData = null;
    infoIf(LOG, () -> "status: " + nodePath + " -> " + statusCode.name());
    Optional<ZoolWatcher> maybeZoolWatcher = Optional.ofNullable(zoolWatcher);
    if (statusCode == Code.OK) {
      try {
        newData = zk.getData(nodePath, false, null);
      } catch (KeeperException e) {
        // We don't need to worry about recovering now. The signal
        // callbacks will kick off any exception handling
        LOG.error("Could not get new data at: " + nodePath, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while fetching new data from: " + nodePath, e);
        zoolWatcher.onDataNotExists(nodePath);
        return;
      }

      if ((newData == null && null != prevData) || (newData != null && !Arrays.equals(prevData, newData))) {
        final byte[] nd = newData;
        maybeZoolWatcher.ifPresent(zwatcher -> zwatcher.onData(this.nodePath, nd));
        prevData = newData;
      }

      maybeZoolWatcher.ifPresent(zwatcher -> {
        if (zwatcher.isReadChildren()) {
          infoIf(LOG, () -> "getting children of " + nodePath);
          final List<String> children = ZoolSystemUtil.getChildNodesAtPath(zk, nodePath, false);
          if (children.isEmpty()) {
            zwatcher.onNoChildren(nodePath);
          } else {
            zwatcher.onChildren(nodePath, children);
          }
        }
      });
    } else if (statusCode == Code.NONODE) {
      maybeZoolWatcher.ifPresent(zwatcher -> zwatcher.onDataNotExists(nodePath));
    } else {
      LOG.warn("Unhandled status code: " + statusCode.name());
    }
  }

  @Override
  public void burn() {
    LOG.warn("Shutting down data bridge: " + nodePath);
    die(Code.OK);
  }

  /**
   * Die command.
   *
   * @param code the reason code.
   */
  private void die(Code code) {
    LOG.warn("Shutting down ZK Monitor: " + nodePath);
    dead = true;
    if (zoolWatcher != null) {
      zoolWatcher.onZoolSessionInvalid(code, this.nodePath);
    }
  }

  @Override
  public boolean isDead() {
    return dead;
  }
}