package com.decoded.zool;

/**
 * A simple class that monitors the data and existence of a ZooKeeper node. It uses asynchronous ZooKeeper APIs.
 */

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import play.Logger;

import java.util.Arrays;


/**
 * Watcher and Stat Callback for Zookeeper DataSink objects.
 */
public class ZookeeperDataSinkBridge implements Watcher,
    StatCallback {
  private static final Logger.ALogger LOG = Logger.of(ZookeeperDataSinkBridge.class);
  private ZooKeeper zk;
  private String zkNodePath;
  private boolean dead;
  private ZookeeperEventListener zookeeperEventListener;
  private byte[] prevData;

  /**
   * Constructor
   *
   * @param zk                     {@link ZooKeeper} instance
   * @param zkNodePath             the path to a node to monitor on the server
   * @param zookeeperEventListener the listener to handle events.
   */
  public ZookeeperDataSinkBridge(ZooKeeper zk, String zkNodePath, ZookeeperEventListener zookeeperEventListener) {
    this.zk = zk;
    this.zkNodePath = zkNodePath;
    this.zookeeperEventListener = zookeeperEventListener;
    // Get things started by checking if the node onZNodeData. We are going
    // to be completely event driven
    watch();
  }

  @Override
  public void process(WatchedEvent event) {
    final String path = event.getPath();
    LOG.info("process watched at path: " + path + ", type: " + event.getType() + ", state: " + event.getState());
    if (event.getType() == Event.EventType.None) {
      // We are are being told that the state of the
      // connection has changed
      switch (event.getState()) {
        case SyncConnected:
          // In this particular example we don't need to do anything
          // here - watches are automatically re-registered with
          // server and any watches triggered while the client was
          // disconnected will be delivered (in order of course)
          LOG.info("Connected status received");
          break;
        case Expired:
          // disconnected from the
          die(Code.SESSIONEXPIRED);
          break;
        default:
          break;
      }
    } else {
      // Something has changed on the node, let's find out
      if (path != null && path.equals(zkNodePath)) {
        watch();
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
        watch();
        return;
    }

    status(code);
  }

  private void watch() {
    if(zk != null) {
      LOG.info("Watching: " + zkNodePath);
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
        // We don't need to worry about recovering now. The watch
        // callbacks will kick off any exception handling
        LOG.error("Could not get new data at: " + zkNodePath, e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while fetching new data from: " + zkNodePath, e);
        return;
      }

      if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
        LOG.info("New data is ready..." + this.zkNodePath + ", size: " + b.length);
        if(zookeeperEventListener != null) {
          zookeeperEventListener.onZNodeData(b, this.zkNodePath);
        }
        prevData = b;
      }
    } else if (statusCode == Code.NONODE) {
      if(zookeeperEventListener != null) {
        zookeeperEventListener.onZNodeDataNotExists(zkNodePath);
      }
    }
  }

  void die(Code code) {
    LOG.warn("Shutting down ZK Monitor: " + zkNodePath);
    dead = true;
    if(zookeeperEventListener != null) {
      zookeeperEventListener.onZookeeperSessionInvalid(code, this.zkNodePath);
    }
  }

  /**
   * True if the monitor is dead.
   *
   * @return a boolean.
   */
  public boolean isDead() {
    return dead;
  }
}