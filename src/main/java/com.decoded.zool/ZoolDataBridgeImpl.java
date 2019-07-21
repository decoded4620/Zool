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
import static com.decoded.zool.ZoolLoggingUtil.infoT;


/**
 * Watcher and Stat Callback for Zookeeper DataSink objects.
 */
public class ZoolDataBridgeImpl implements ZoolDataBridge {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolDataBridge.class);
  private ZooKeeper zk;
  private String zkNodePath;
  private boolean dead;
  private ZoolWatcher zoolWatcher;
  private byte[] prevData;
  private final boolean readChildren;


  /**
   * Constructor
   *
   * @param zk          {@link ZooKeeper} instance
   * @param zkNodePath  the path to a node to monitor on the server
   * @param zoolWatcher the listener to handle events.
   * @param readChildren to read or not to read children.
   */
  public ZoolDataBridgeImpl(ZooKeeper zk, String zkNodePath, ZoolWatcher zoolWatcher, boolean readChildren) {
    this.zk = zk;
    this.zkNodePath = zkNodePath;
    this.zoolWatcher = zoolWatcher;
    this.readChildren = readChildren;
    // Get things started by checking if the node onZNodeData. We are going
    // to be completely event driven
    signal(Event.EventType.None, this.readChildren);
  }

  @Override
  public void process(WatchedEvent event) {
    final String path = event.getPath();
    debugIf(LOG, () -> "Processing: " + path + " " + zkNodePath);
    if (event.getType() == Event.EventType.None) {
      if (event.getState().equals(Event.KeeperState.Expired)) {
        die(Code.SESSIONEXPIRED);
      }
    } else {
      // Something has changed on the node, let's find out
      // only handle our own node.
      if (path != null && path.equals(zkNodePath)) {
        debugIf(LOG, () -> "Processing: " + path + " " + zkNodePath);
        signal(event.getType(), this.readChildren);
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
        signal(Event.EventType.None, this.readChildren);
        return;
    }

    if (null != path && path.equals(zkNodePath)) {
      status(code);
    }
  }

  /**
   * Singles for a zookeeper state check for our node path
   */
  private void signal(Event.EventType eventType, boolean readChildren) {
    if (zk != null) {
      infoT(LOG, "signal on event " + eventType.name() + ": " + zkNodePath);
      zk.exists(zkNodePath, true, this, null);

      if (eventType.equals(Event.EventType.NodeChildrenChanged) || readChildren) {
        debugIf(LOG, () -> "Node Children Changed on " + zkNodePath);
        try {
          List<String> children = zk.getChildren(zkNodePath, true);

          Optional.ofNullable(zoolWatcher).ifPresent(zwatcher -> {
            if (children.isEmpty()) {
              zwatcher.onNoChildren(zkNodePath);
            } else {
              zwatcher.onChildren(zkNodePath, children);
            }
          });
        } catch (KeeperException ex) {
          LOG.error("Could not get children, zookeeper error", ex);

        } catch (InterruptedException ex) {
          LOG.error("Interrupted while fetching children", ex);
        }
      }
    }
  }


  /**
   * Safely returns child nodes at a specific path.
   * @param path the path to get children from.
   * @param watch boolean
   * @return a list of Strings.
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
    infoT(LOG, "status: " + zkNodePath + " -> " + statusCode.name());
    Optional<ZoolWatcher> maybeZoolWatcher = Optional.ofNullable(zoolWatcher);
    if (statusCode == Code.OK) {
      try {
        newData = zk.getData(zkNodePath, false, null);
      } catch (KeeperException e) {
        // We don't need to worry about recovering now. The signal
        // callbacks will kick off any exception handling
        LOG.error("Could not get new data at: " + zkNodePath, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while fetching new data from: " + zkNodePath, e);
        zoolWatcher.onDataNotExists(zkNodePath);
        return;
      }

      if ((newData == null && null != prevData) || (newData != null && !Arrays.equals(prevData, newData))) {
        final byte[] nd = newData;
        maybeZoolWatcher.ifPresent(zwatcher -> zwatcher.onData(this.zkNodePath, nd));
        prevData = newData;
      }

      maybeZoolWatcher.ifPresent(zwatcher -> {
        if (zwatcher.isReadChildren()) {
          infoT(LOG, "getting children of " + zkNodePath);
          final List<String> children = ZoolSystemUtil.getChildNodesAtPath(zk, zkNodePath, false);
          if (children.isEmpty()) {
            zwatcher.onNoChildren(zkNodePath);
          } else {
            zwatcher.onChildren(zkNodePath, children);
          }
        }
      });
    } else if (statusCode == Code.NONODE) {
      maybeZoolWatcher.ifPresent(zwatcher -> zwatcher.onDataNotExists(zkNodePath));
    } else {
      LOG.warn("Unhandled status code: " + statusCode.name());
    }
  }

  /**
   * Die command.
   *
   * @param code the reason code.
   */
  private void die(Code code) {
    LOG.warn("Shutting down ZK Monitor: " + zkNodePath);
    dead = true;
    if (zoolWatcher != null) {
      zoolWatcher.onZoolSessionInvalid(code, this.zkNodePath);
    }
  }

  @Override
  public boolean isDead() {
    return dead;
  }
}