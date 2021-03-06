package com.decoded.zool;

import com.decoded.zool.connection.ZookeeperConnection;
import com.decoded.zool.dataflow.DataFlowState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.decoded.zool.ZoolLoggingUtil.debugIf;
import static com.decoded.zool.ZoolLoggingUtil.infoIf;


/**
 * The {@link ZoolDataFlow} accepts Zookeeper data, and directs it to each DataSink listening for data via node name.
 * {@link ZoolDataFlow} is a 1:1 object with a zookeeper instance. Meaning it handles all nodes from the root of that
 * Zookeeper. When running multiple ZooKeeper Quorums, you can create multiple DataFlow. Each will be its own silo of
 * zookeeper data and events. The default binding is an eager singleton
 * <p>
 * <code>
 * bind(ZoolDataFlow.class).to(ZoolDataFlowImpl.class).asEagerSingleton();
 * </code>
 * <p>
 * However, Guice provides injection capabilities to do named singletons etc. You can use the
 * <code>setHost(String)</code> and
 * <code>setPort(int)</code> methods to update the connection string (before calling <code>connect()</code>!)
 * <p>
 * For details on the basic API:
 *
 * @see ZoolDataFlow
 */
public class ZoolDataFlowImpl implements ZoolDataFlow {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolDataFlowImpl.class);

  private ZooKeeper zk;
  private Thread dataFlowThread;

  private Map<String, ZoolDataBridge> zoolDataBridgeMap = new ConcurrentHashMap<>();
  private Map<String, List<ZoolDataSink>> dataSinkMap = new ConcurrentHashMap<>();
  private ExecutorService executorService;

  private boolean connected = false;

  // This handles data from all nodes.
  private String zNode = ZConst.PathSeparator.ZK.sep();
  private String name = ZoolDataFlow.class.getName();

  private DataFlowState state = DataFlowState.DISCONNECTED;
  private final ZookeeperConnection zookeeperConnection;

  @Inject
  public ZoolDataFlowImpl(ZookeeperConnection zookeeperConnection, ExecutorService executorService) {
    this.zookeeperConnection = zookeeperConnection;
    this.executorService = executorService;
  }

  @Override
  public ZookeeperConnection getZookeeperConnection() {
    return zookeeperConnection;
  }

  public DataFlowState getState() {
    return state;
  }

  @Override
  public ZoolDataSinkImpl setReadChildren(final boolean readChildren) {
    throw new IllegalStateException("Cannot set read children on a DataFlow, only on Data Sinks.");
  }

  @Override
  public boolean isReadChildren() {
    // if any data sink on any channel is read children, return true.
    List<List<ZoolDataSink>> values = ImmutableList.copyOf(dataSinkMap.values());
    boolean atLeastOneDataSinkReadsChildren = values.stream().anyMatch(dataSinkList -> dataSinkList.stream().anyMatch(ZoolDataSink::isReadChildren));
    debugIf(LOG, () -> "Is dataflow reading children: " + atLeastOneDataSinkReadsChildren);
    return atLeastOneDataSinkReadsChildren;
  }

  @Override
  public ZoolDataSink disconnectAfterLoadComplete() {
    throw new NotImplementedException("Cannot use this method on Zool Data Flow");
  }

  @Override
  public boolean isDisconnectingAfterLoadComplete() {
    return false;
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
  public void connect() {
    infoIf(LOG, () -> "Connecting...");
    executorService.submit(this);
  }

  private void checkZoo() {
    infoIf(LOG, () -> "Check zookeeper");
    if (zk == null) {
      infoIf(LOG, () -> "Creating and connecting to zookeeper");
      state = DataFlowState.CONNECTING;
      zk = createZookeeper();
      // if the above times out / throws an exception, connection will be reset.
      state = zk == null
          ? DataFlowState.DISCONNECTED
          : (zk.getState().isConnected() ? DataFlowState.CONNECTED : DataFlowState.DISCONNECTED);
    } else {
      if (zk.getState().isConnected()) {
        state = DataFlowState.CONNECTED;
      } else {
        state = DataFlowState.DISCONNECTED;
      }
    }

    infoIf(LOG, () -> "Zookeeper State: " + state);
  }

  @Override
  public ZoolDataSink setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler) {
    throw new IllegalStateException("Cannot set child nodes handler on DataFlow, only DataSink");
  }

  @Override
  public ZoolDataSink setNoChildNodesHandler(final Consumer<String> noChildNodesHandler) {
    throw new IllegalStateException("Cannot set no child nodes handler on DataFlow, only DataSink");
  }

  /**
   * Add a watch on a specific node.
   *
   * @param dataSink the dataSink to watch
   */
  @VisibleForTesting
  void watch(ZoolDataSink dataSink) {
    checkZoo();
    zoolDataBridgeMap.computeIfAbsent(dataSink.getZNode(), zN -> this.createZoolDataBridge(dataSink));
  }

  /**
   * Stop watching the node.
   *
   * @param zNode the node path
   *
   * @return true if watching stopped
   */
  @VisibleForTesting
  boolean unwatch(String zNode) {
    checkZoo();
    try {
      infoIf(LOG, () -> "Unwatching zool node: " + zNode);
      zk.removeWatches(zNode, this, WatcherType.Any, true);
      infoIf(LOG, () -> "Unwatched zool node: " + zNode + " successfully");
      return true;
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while unwatching zool node: " + zNode);
      state = DataFlowState.DISCONNECTED;
    } catch (KeeperException ex) {
      // do nothing
      LOG.warn("Keeper Exception while unwatching zool node: ", ex);
    }
    return false;
  }

  @VisibleForTesting
  ZooKeeper createZookeeper() {
    try {
      debugIf(LOG, () -> "Creating a Zookeeper on " + zookeeperConnection.getZkHostAddress() + ", with negotiated timeout " + zookeeperConnection.getZkConnectTimeout());
      return new ZooKeeper(zookeeperConnection.getZkHostAddress(), zookeeperConnection.getZkConnectTimeout(), this);
    } catch (IOException ex) {
      LOG.error("Error creating Zookeeper");
      return null;
    }
  }

  @VisibleForTesting
  ZoolDataBridge createZoolDataBridge(ZoolDataSink dataSink) {
    debugIf(LOG, () -> "Creating a Data Bridge for zNode: " + dataSink.getZNode());
    return new ZoolDataBridgeImpl(zk, dataSink.getZNode(), this, dataSink.isReadChildren());
  }

  @Override
  public void drain(ZoolDataSink dataSink) {
    debugIf(LOG, () -> "Draining to Data Sink: " + dataSink.getName());
    dataSinkMap.computeIfAbsent(dataSink.getZNode(), p -> new CopyOnWriteArrayList<>()).add(dataSink);
    // add a watch if not added
    watch(dataSink);
  }

  @Override
  public void drain(final String path,
      final boolean watch,
      final BiConsumer<String, byte[]> dataHandler,
      Object inputContext) {
    if (!dataSinkMap.containsKey(path)) {
      LOG.warn("Warning, listening to a path " + path + " which has no data sink or channel active");
    }
    checkZoo();
    zk.getData(path, watch, (rc, p, ctx, d, s) -> {
      infoIf(LOG, () -> "Draining to path: " + p + " data: " + d.length);
      dataHandler.accept(p, d);
    }, inputContext);
  }

  @Override
  public void drainStop(ZoolDataSink dataSink) {
    debugIf(LOG, () -> "Stop draining to Data Sink: [" + dataSink.getZNode() + "]");
    List<ZoolDataSink> handlersAtPath = dataSinkMap.computeIfAbsent(dataSink.getZNode(),
        p -> new CopyOnWriteArrayList<>());

    handlersAtPath.remove(dataSink);
    if (handlersAtPath.isEmpty()) {
      if (!unwatch(dataSink.getZNode())) {
        LOG.error("Could not unwatch data from datasink: " + dataSink.getZNode());
      }
    }
  }

  @Override
  public boolean delete(final String path) {
    checkZoo();
    debugIf(LOG, () -> "Deleting node at " + path);
    try {
      Stat stat = zk.exists(path, false);
      if (stat != null) {
        zk.delete(path, stat.getVersion());
      }
    } catch (KeeperException.ConnectionLossException ex) {
      infoIf(LOG, () -> "Zookeeper disconnected " + ex.getMessage());
      return false;
    } catch (KeeperException ex) {
      LOG.error("Keeper Exception", ex);
      return false;
      // if can't create, try to remove our node.
    } catch (InterruptedException ex) {
      LOG.error("Interrupted exception", ex);
      state = DataFlowState.DISCONNECTED;
      return false;
    }

    return true;
  }

  @Override
  public boolean update(final String path, final byte[] data) {
    checkZoo();
    try {
      final Stat stat = zk.exists(path, false);

      if (stat != null) {
        Stat setStat = zk.setData(path, data, stat.getVersion());
        infoIf(LOG,
            () -> "updated " + path + ", to version: " + setStat.getVersion() + ", with data: " + data.length + " " +
                "bytes");
        return true;
      }

      LOG.error("Node " + path + " doesn't exist, create it first");
    } catch (KeeperException.ConnectionLossException ex) {
      infoIf(LOG, () -> "Zookeeper disconnected: " + ex.getMessage());
    } catch (KeeperException ex) {
      LOG.error("Keeper Exception: ", ex);
    } catch (InterruptedException ex) {
      LOG.error("ZooKeeper Interruption ", ex);
      state = DataFlowState.DISCONNECTED;
    }

    LOG.warn("Node " + path + " was NOT updated");
    return false;
  }

  @Override
  public boolean nodeExists(final String path) {
    checkZoo();
    try {
      return zk.exists(path, false) != null;
    } catch (KeeperException.ConnectionLossException ex) {
      infoIf(LOG, () -> "Zookeeper disconnected: " + ex.getMessage());
      return false;
    } catch (KeeperException ex) {
      LOG.warn("Keeper Exception", ex);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted", ex);
      state = DataFlowState.DISCONNECTED;
    }
    infoIf(LOG, () -> "Node " + path + " does not exist");
    return false;
  }

  /**
   * Watch children of a newly created node
   *
   * @param path path to the parent node we're creating a child on.
   */
  private void watchChildren(String path) {
    infoIf(LOG, () -> "Watching children of: " + path);
    Optional.ofNullable(dataSinkMap.get(path)).ifPresent(dataSinks -> {
      if (dataSinks.stream().anyMatch(ZoolDataSink::isReadChildren)) {
        // add a watch for the node to be updated
        infoIf(LOG, () -> "Watching Child Nodes at Path: " + path);
        getChildNodesAtPath(path, true);
      }
    });
  }

  @Override
  public boolean create(final String path, final byte[] data, final List<ACL> acls, final CreateMode mode) {
    checkZoo();
    try {
      Stat stat = zk.exists(path, false);
      if (stat == null) {
        try {
          zk.create(path, data, acls, mode);
          infoIf(LOG, () -> "Node " + path + " created");
        } catch (KeeperException.NodeExistsException ex) {
          infoIf(LOG, () -> "Node: " + path + " already exists: " + ex.getMessage());
        }

        watchChildren(path);
        return true;
      }
    } catch (KeeperException.ConnectionLossException ex) {
      infoIf(LOG, () -> "Zookeeper disconnected: " + ex.getMessage());
    } catch (KeeperException ex) {
      LOG.warn("Keeper exception creating node: {}", ex.getMessage());
    } catch (InterruptedException ex) {
      LOG.error("Interrupted exception", ex);
      state = DataFlowState.DISCONNECTED;
    }

    return false;
  }

  @Override
  public byte[] get(final String path) {
    checkZoo();
    try {
      if (zk.exists(path, false) != null) {
        return zk.getData(path, false, null);
      }
    } catch (KeeperException.ConnectionLossException ex) {
      infoIf(LOG, () -> "Zookeeper disconnected: " + ex.getMessage());
    } catch (KeeperException ex) {
      LOG.error("Keeper exception finding node: ", ex);
      state = DataFlowState.DISCONNECTED;
    } catch (InterruptedException ex) {
      LOG.warn("Keeper Interrupted", ex);
      state = DataFlowState.DISCONNECTED;
    }
    return new byte[0];
  }

  @Override
  public List<String> getChildNodesAtPath(final String path, final boolean watch) {
    checkZoo();
    infoIf(LOG, () -> "get child nodes at path: " + path + ", watch " + watch);
    return ZoolSystemUtil.getChildNodesAtPath(zk, path, watch);
  }

  @Override
  public void process(WatchedEvent event) {
    debugIf(LOG, () -> "Processing zk event: [" + event.getPath() + "], " + event.getType());
    zoolDataBridgeMap.values().iterator().forEachRemaining(watch -> watch.process(event));
  }

  private boolean allDead() {
    Iterator<ZoolDataBridge> it = zoolDataBridgeMap.values().iterator();
    boolean allDead = true;
    while (it.hasNext()) {
      ZoolDataBridge bridge = it.next();
      if (!bridge.isDead()) {
        infoIf(LOG, () -> "Not dead yet");
        allDead = false;

        break;
      }
    }

    return allDead;
  }

  @Override
  public void run() {
    infoIf(LOG, () -> "Starting ZoolDataFlow: [" + zNode + "]");
    try {
      synchronized (this) {
        dataFlowThread = Thread.currentThread();
        while (!allDead()) {
          state = DataFlowState.CONNECTED;
          connected = true;
          wait();
        }
        connected = false;
        state = DataFlowState.DISCONNECTED;
        dataFlowThread = null;
      }
    } catch (InterruptedException e) {
      LOG.warn("ZoolDataFlow is Shutting Down");
      state = DataFlowState.DISCONNECTED;
      connected = false;
    }
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public void terminate() {
    LOG.warn(
        "Terminating ZoolDataFlow: [" + zNode + "] and " + zoolDataBridgeMap.size() + " watchers and " + dataSinkMap.size() + " sinks");
    zoolDataBridgeMap.values().iterator().forEachRemaining(bridge -> {
      unwatch(bridge.getNodePath());
      bridge.burn();
    });

    if (dataFlowThread != null) {
      dataFlowThread.interrupt();
    }

    try {
      zk.close();
    } catch (InterruptedException ex) {
      LOG.error("Could not close zk connection");
    }
    connected = false;
  }

  @Override
  public void onZoolSessionInvalid(KeeperException.Code rc, String nodePath) {
    synchronized (this) {
      LOG.warn("onZoolSessionInvalid: " + nodePath);
      notifyAll();
    }
  }

  @Override
  public void onNoChildren(final String zNode) {
    infoIf(LOG, () -> "onNoChildren(" + zNode + ") Node is " + (dataSinkMap.containsKey(zNode)
        ? "Watched!"
        : "Not Watched!"));
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.stream()
            .filter(ZoolDataSink::isReadChildren)
            .forEach(handler -> handler.onNoChildren(zNode)));
  }

  @Override
  public void onChildren(final String zNode, final List<String> childNodes) {
    infoIf(LOG, () -> "onChildren(" + zNode + " ==> " + childNodes.size() + ") -> Node is " + (dataSinkMap.containsKey(zNode)
        ? "Watched!"
        : "Not Watched!"));
    // remove each of the data sinks that should disconnect when no data exists.
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.stream()
            .filter(ZoolDataSink::isReadChildren)
            .forEach(handler -> handler.onChildren(zNode, childNodes)));
  }

  @Override
  public void onDataNotExists(String zNode) {
    infoIf(LOG, () -> "onDataNotExists(" + zNode + ")");
    // remove each of the data sinks that should disconnect when no data exists.
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.forEach(handler -> handler.onDataNotExists(zNode)));
  }

  @Override
  public void onData(String zNode, byte[] data) {
    infoIf(LOG, () -> "onData(" + zNode + ", " + data.length + ")");
    // remove each of the data sinks that should disconnect when no data exists.
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(dataSinkList -> dataSinkList.forEach(dataSink -> dataSink.onData(dataSink.getZNode(), data)));
  }

  @Override
  public void onLoadComplete(final String zNode) {
    infoIf(LOG, () -> "onLoadComplete(" + zNode + ")");
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.stream()
            .peek(handler -> handler.onLoadComplete(zNode))
            .filter(ZoolDataSink::isDisconnectingAfterLoadComplete)
            .collect(Collectors.toList())
            .forEach(this::drainStop));
  }
}