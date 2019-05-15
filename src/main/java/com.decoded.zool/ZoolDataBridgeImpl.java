package com.decoded.zool;

/**
 * A simple class that monitors the data and existence of a ZooKeeper node. It uses asynchronous ZooKeeper APIs.
 */

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * Watcher and Stat Callback for Zookeeper DataSink objects.
 */
public class ZoolDataBridgeImpl implements
    ZoolDataBridge {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolDataBridgeImpl.class);
  private ZooKeeper zk;
  private String zkNodePath;
  private boolean dead;
  private ZoolWatcher zoolWatcher;
  private byte[] prevData;

  /**
   * Constructor
   *
   * @param zk          {@link ZooKeeper} instance
   * @param zkNodePath  the path to a node to monitor on the server
   * @param zoolWatcher the listener to handle events.
   */
  public ZoolDataBridgeImpl(ZooKeeper zk, String zkNodePath, ZoolWatcher zoolWatcher) {
    this.zk = zk;
    this.zkNodePath = zkNodePath;
    this.zoolWatcher = zoolWatcher;
    // Get things started by checking if the node onZNodeData. We are going
    // to be completely event driven
    signal();
  }

  @Override
  public void process(WatchedEvent event) {
    final String path = event.getPath();
    LOG.info("process watched at path: " + path + ", type: " + event.getType() + ", state: " + event.getState());
    if (event.getType() == Event.EventType.None) {
      if (event.getState().equals(Event.KeeperState.Expired)) {
        die(Code.SESSIONEXPIRED);
      }
    } else {
      // Something has changed on the node, let's find out
      // only handle our own node.
      if (path != null && path.equals(zkNodePath)) {
        signal();
      }
    }
  }

  @Override
  public void processResult(int rc, String path, Object ctx, Stat stat) {
    final Code code = Code.get(rc);

    LOG.info("---------------------------------------------------------");
    LOG.warn("Process result: " + code.name() + ", " + path);
    LOG.info("---------------------------------------------------------");
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
        signal();
        return;
    }

    status(code);
  }

  private void signal() {
    if (zk != null) {
      zk.exists(zkNodePath, true, this, null);
    }
  }

  private void status(Code statusCode) {
    LOG.info("Status: " + zkNodePath + ", code: " + statusCode.name());
    byte[] b = null;
    if (statusCode == Code.OK) {
      try {
        b = zk.getData(zkNodePath, false, null);
        LOG.info("Got new data at: " + zkNodePath);
      } catch (KeeperException e) {
        // We don't need to worry about recovering now. The signal
        // callbacks will kick off any exception handling
        LOG.error("Could not get new data at: " + zkNodePath, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while fetching new data from: " + zkNodePath, e);
        return;
      }

      if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
        LOG.info("New data is ready..." + this.zkNodePath + ", size: " + b.length);
        if (zoolWatcher != null) {
          zoolWatcher.onData(this.zkNodePath, b);
        }
        prevData = b;
      }
    } else if (statusCode == Code.NONODE) {
      if (zoolWatcher != null) {
        zoolWatcher.onDataNotExists(zkNodePath);
      }
    }
  }

  void die(Code code) {
    LOG.warn("Shutting down ZK Monitor: " + zkNodePath);
    dead = true;
    if (zoolWatcher != null) {
      zoolWatcher.onZoolSessionInvalid(code, this.zkNodePath);
    }
  }

  /**
   * True if the monitor is dead.
   *
   * @return a boolean.
   */
  @Override
  public boolean isDead() {
    return dead;
  }
}