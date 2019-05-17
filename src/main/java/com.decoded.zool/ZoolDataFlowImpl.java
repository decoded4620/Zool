package com.decoded.zool;

/**
 * A simple example program to use ZoolDataBridgeImpl to start and stop executables based on a znode. The program dataSinkBridgeMap
 * the specified znode and saves the data that corresponds to the znode in the filesystem. It also starts the specified
 * program with the specified arguments when the znode onZNodeData and kills the program if the znode goes away.
 */

import com.google.common.annotations.VisibleForTesting;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;


/**
 * The {@link ZoolDataFlow} accepts Zookeeper data, and directs it to each DataSink listening for data via node name.
 */
public class ZoolDataFlowImpl implements
    ZoolDataFlow {
  private Logger LOG = LoggerFactory.getLogger(ZoolDataFlowImpl.class);

  private ZooKeeper zk;
  private Thread dataFlowThread;
  private String host = "localhost";
  private int port = 2181;
  private int timeout = Integer.MAX_VALUE;

  private Map<String, ZoolDataBridge> dataSinkBridgeMap = new HashMap<>();
  private Map<String, List<ZoolDataSink>> dataSinkMap = new HashMap<>();
  private ExecutorService executorService;

  // This handles data from all nodes.
  private String zNode = "/";
  private String name = ZoolDataFlow.class.getName();

  @Inject
  public ZoolDataFlowImpl(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getZNode() {
    return zNode;
  }

  @Override
  public ZoolDataFlowImpl setHost(String host) {
    this.host = host;
    return this;
  }

  @Override
  public ZoolDataFlowImpl setPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public ZoolDataFlowImpl setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public void connect() {
    executorService.submit(this);
  }

  /**
   * Add a watch on a specific node.
   *
   * @param zNode the path to watch
   * @return true if watching started.
   */
  @VisibleForTesting
  boolean watch(String zNode) {
    if (zk == null) {
      zk = createZookeeper();
    }

    if (zk != null) {
      dataSinkBridgeMap.computeIfAbsent(zNode, this::createDataBridge);
      return true;
    }

    LOG.error("cannot watch " + zNode + " on host: " + host + ':' + port);
    return false;
  }

  /**
   * Stop watching the node.
   *
   * @param zNode the node path
   * @return true if watching stopped
   */
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
  ZoolDataBridge createDataBridge(String zNode) {
    return new ZoolDataBridgeImpl(zk, zNode, this);
  }

  @Override
  public void drain(ZoolDataSink dataSink) {
    dataSinkMap.computeIfAbsent(dataSink.getZNode(), p -> new ArrayList<>()).add(dataSink);
    // add a watch if not added
    if (!watch(dataSink.getZNode())) {
      LOG.error("Could not accept data from sink: " + dataSink.getName());
    }
  }

  @Override
  public void drainStop(ZoolDataSink dataSink) {
    List<ZoolDataSink> handlersAtPath = dataSinkMap.computeIfAbsent(dataSink.getZNode(), p -> new ArrayList<>());

    handlersAtPath.remove(dataSink);
    if (handlersAtPath.isEmpty()) {
      if (!
          unwatch(dataSink.getZNode())) {
        LOG.error("Could not accept data form sink: " + dataSink.getName());
      }
    }
  }

  @Override
  public ZooKeeper getZk() {
    return zk;
  }

  @Override
  public void process(WatchedEvent event) {
    dataSinkBridgeMap.values().iterator().forEachRemaining(watch -> watch.process(event));
  }

  private boolean allDead() {
    Iterator<ZoolDataBridge> it = dataSinkBridgeMap.values().iterator();
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
      LOG.warn("ZoolDataFlow is Shutting Down");
    }
  }

  @Override
  public void terminate() {
    if (dataFlowThread != null) {
      dataFlowThread.interrupt();
    }
  }

  @Override
  public void onZoolSessionInvalid(KeeperException.Code rc, String nodePath) {
    synchronized (this) {
      LOG.warn("onZoolSessionInvalid: " + nodePath);
      notifyAll();
    }
  }

  @Override
  public void onDataNotExists(String zNode) {
    Optional.ofNullable(dataSinkMap.get(zNode)).ifPresent(handlers -> handlers.forEach(handler -> handler.onDataNotExists(zNode)));
  }

  @Override
  public void onData(String zNode, byte[] data) {
    Optional<List<ZoolDataSink>> maybeHandlers = Optional.ofNullable(dataSinkMap.get(zNode));
    
    final boolean empty = data.length == 0;
    maybeHandlers.ifPresent(handlers ->
        handlers.forEach(handler -> {
          if (empty) {
            handler.onDataNotExists(zNode);
          } else {
            handler.onData(zNode, data);
          }
        }));
  }
}