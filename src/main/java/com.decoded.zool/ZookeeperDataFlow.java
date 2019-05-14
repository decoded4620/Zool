package com.decoded.zool;

/**
 * A simple example program to use ZookeeperDataSinkBridge to start and stop executables based on a znode. The program dataSinkBridgeMap
 * the specified znode and saves the data that corresponds to the znode in the filesystem. It also starts the specified
 * program with the specified arguments when the znode onZNodeData and kills the program if the znode goes away.
 */

import com.google.common.annotations.VisibleForTesting;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import play.Logger;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;


/**
 * This object handles Data from Zookeeper
 */
public class ZookeeperDataFlow implements Watcher,
    Runnable,
    ZookeeperEventListener {
  private Logger LOG = Logger.getLogger(ZookeeperDataFlow.class.getName());

  private ZooKeeper zk;
  private Thread dataFlowThread;
  private String host = "localhost";
  private int port = 2181;
  private int timeout = Integer.MAX_VALUE;

  private Map<String, ZookeeperDataSinkBridge> dataSinkBridgeMap = new HashMap<>();
  private Map<String, List<ZookeeperDataSink>> dataSinkMap = new HashMap<>();
  private ExecutorService executorService;


  @Inject
  public ZookeeperDataFlow(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public ZookeeperDataFlow setHost(String host) {
    this.host = host;
    return this;
  }

  public ZookeeperDataFlow setPort(int port) {
    this.port = port;
    return this;
  }

  public ZookeeperDataFlow setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Connects the data flow.
   */
  public void connect() {
    executorService.submit(this);
  }

  /**
   * Add a watch on a specific node.
   *
   * @param zNode the path to watch
   */
  @VisibleForTesting
  boolean watch(String zNode) {
    if (zk == null) {
      zk = createZookeeper();
    }

    if (zk != null) {
      dataSinkBridgeMap.computeIfAbsent(zNode, this::createZookeeperMonitor);
      return true;
    }

    LOG.error("cannot watch " + zNode + " on host: " + host + ':' + port);
    return false;
  }

  @VisibleForTesting
  boolean unwatch(String zNode) {
    try {
      zk.removeWatches(zNode, this, WatcherType.Any, true);
      return true;
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while removing dataSinkBridgeMap at path: " + zNode);
    } catch (KeeperException ex) {
      LOG.warn("Zookeeper could not remove watch " + zNode, ex);
    }
    return false;
  }

  @VisibleForTesting
  ZooKeeper createZookeeper() {
    try {
      return new ZooKeeper(host + ':' + port, timeout, this);
    } catch (IOException ex) {
      LOG.error("Error creating Zookeeper");
      return null;
    }
  }

  @VisibleForTesting
  ZookeeperDataSinkBridge createZookeeperMonitor(String zNode) {
    return new ZookeeperDataSinkBridge(zk, zNode, this);
  }

  public void drain(ZookeeperDataSink dataSink) {
    LOG.info("drain: " + dataSink.getPath() + ", handler: " + dataSink);

    dataSinkMap.computeIfAbsent(dataSink.getPath(), p -> new ArrayList<>()).add(dataSink);
    // add a watch if not added
    watch(dataSink.getPath());
  }

  public void drainStop(ZookeeperDataSink dataSink) {
    LOG.info("drainStop: " + dataSink.getPath());

    List<ZookeeperDataSink> handlersAtPath = dataSinkMap.computeIfAbsent(dataSink.getPath(), p -> new ArrayList<>());

    handlersAtPath.remove(dataSink);
    if (handlersAtPath.isEmpty()) {
      unwatch(dataSink.getPath());
    }
  }

  public ZooKeeper getZk() {
    return zk;
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.info("process event: " + event.getPath());
    dataSinkBridgeMap.values().iterator().forEachRemaining(watch -> watch.process(event));
  }

  private boolean allDead() {
    Iterator<ZookeeperDataSinkBridge> it = dataSinkBridgeMap.values().iterator();
    boolean allDead = true;
    while (it.hasNext()) {
      if (!it.next().isDead()) {
        allDead = false;
        break;
      }
    }

    return allDead;
  }

  @Override
  public void run() {
    try {
      synchronized (this) {
        dataFlowThread = Thread.currentThread();
        while (!allDead()) {
          wait();
        }
        dataFlowThread = null;
      }
    } catch (InterruptedException e) {
      LOG.warn("Shutting Down");
    }
  }

  public void terminate() {
    if (dataFlowThread != null) {
      dataFlowThread.interrupt();
    }
  }

  public void onZookeeperSessionInvalid(KeeperException.Code rc, String nodePath) {
    synchronized (this) {
      LOG.warn("onZookeeperSessionInvalid: " + nodePath);
      notifyAll();
    }
  }

  @Override
  public void onZNodeDataNotExists(String nodePath) {
    LOG.info("onZNodeDataNotExists: " + nodePath);
    Optional.ofNullable(dataSinkMap.get(nodePath)).ifPresent(handlers -> handlers.forEach(handler -> handler.onDataNotExists(nodePath)));
  }

  @Override
  public void onZNodeData(byte[] data, String nodePath) {
    LOG.info("onZNodeData: " + data.length + ", " + nodePath);

    Optional<List<ZookeeperDataSink>> maybeHandlers = Optional.ofNullable(dataSinkMap.get(nodePath));

    maybeHandlers.ifPresent(handlers -> {
      handlers.forEach(handler -> {
        if (data.length == 0) {
          handler.onDataNotExists(nodePath);
        } else {
          handler.onData(nodePath, data);
        }
      });
    });
  }
}