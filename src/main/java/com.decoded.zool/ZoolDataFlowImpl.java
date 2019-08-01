package com.decoded.zool;

import com.decoded.zool.dataflow.DataFlowState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private String host = "localhost";
  private int port = 2181;
  private int timeout = Integer.MAX_VALUE;

  private Map<String, ZoolDataBridge> zoolDataBridgeMap = new ConcurrentHashMap<>();
  private Map<String, List<ZoolDataSink>> dataSinkMap = new ConcurrentHashMap<>();
  private ExecutorService executorService;

  private boolean connected = false;

  // This handles data from all nodes.
  private String zNode = ZConst.PathSeparator.ZK.sep();
  private String name = ZoolDataFlow.class.getName();

  private DataFlowState state = DataFlowState.DISCONNECTED;


  @Inject
  public ZoolDataFlowImpl(ExecutorService executorService) {
    this.executorService = executorService;
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

    return values.stream().anyMatch(dataSinkList -> dataSinkList.stream().anyMatch(ZoolDataSink::isReadChildren));
  }

  @Override
  public ZoolDataSink disconnectWhenDataIsReceived() {
    return this;
  }

  @Override
  public ZoolDataSink disconnectWhenNoDataExists() {
    return this;
  }

  @Override
  public boolean willDisconnectOnData() {
    return false;
  }

  @Override
  public boolean willDisconnectOnNoData() {
    return false;
  }

  @Override
  public ZoolDataSink oneOff() {
    return this;
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
    infoIf(LOG, () -> "Connecting...");
    executorService.submit(this);
  }

  private void checkZoo() {
    LOG.info("Check zookeeper");
    if (zk == null) {
      state = DataFlowState.CONNECTING;
      LOG.info("Creating new zookeeper");
      zk = createZookeeper();
      // if the above times out / throws an exception, connection will be reset.
      state = zk == null ? DataFlowState.DISCONNECTED : DataFlowState.CONNECTED;
    } else {
      if (state == DataFlowState.DISCONNECTED) {
        state = DataFlowState.CONNECTING;
      }

      if (zk.getState().isConnected()) {
        state = DataFlowState.CONNECTED;
      } else {
        state = DataFlowState.DISCONNECTED;
      }
    }

    LOG.info("Check zookeeper complete: " + state);
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
   *
   * @return true if watching started.
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
      debugIf(LOG, () -> "Unwatching Zool Node Path: " + zNode);
      zk.removeWatches(zNode, this, WatcherType.Any, true);
      return true;
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while removing zoolDataBridgeMap at path: " + zNode);
      state = DataFlowState.DISCONNECTED;
    } catch (KeeperException ex) {
      // do nothing
    }
    return false;
  }

  @VisibleForTesting
  ZooKeeper createZookeeper() {
    try {
      debugIf(LOG, () -> "Creating a Zookeeper on " + host + ":" + port + ", with negotiated timeout " + timeout);
      return new ZooKeeper(host + ':' + port, timeout, this);
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
        LOG.error("Could not accept data form sink: " + dataSink.getZNode());
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
      Stat stat = zk.exists(path, false);

      if (stat != null) {
        stat = zk.setData(path, data, stat.getVersion());
        LOG.info("updated " + path + ", to version: " + stat.getVersion() + ", with data: " + data.length + " bytes");
        return true;
      }

      LOG.error("Node " + path + " doesn't exist, create it first");
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
    } catch (KeeperException ex) {
      LOG.error("Keeper Exception", ex);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted", ex);
      state = DataFlowState.DISCONNECTED;
    }
    LOG.info("Node " + path + " does not exist");
    return false;
  }

  @Override
  public boolean create(final String path, final byte[] data, final List<ACL> acls, final CreateMode mode) {
    checkZoo();
    try {
      Stat stat = zk.exists(path, false);

      if (stat == null) {
        zk.create(path, data, acls, mode);

        Optional.ofNullable(dataSinkMap.get(path)).ifPresent(dataSinks -> {
          if (dataSinks.stream().anyMatch(ZoolDataSink::isReadChildren)) {
            // add a watch for the node to be updated
            LOG.info("Watching Child Nodes at Path: " + path);
            getChildNodesAtPath(path, true);
          }
        });
        return true;
      }

      LOG.error("Node " + path + " already exists, remove the old, or use update");
    } catch (KeeperException ex) {
      LOG.error("Keeper exception creating node: ", ex);
      // if can't create, try to remove our node.
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
    LOG.info("get child nodes at path: " + path + ", watch " + watch);
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
      if (!it.next().isDead()) {
        allDead = false;

        break;
      }
    }

    return allDead;
  }

  @Override
  public void run() {
    infoIf(LOG, () -> "String ZoolDataFlow: [" + zNode + "]");
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
    if (dataFlowThread != null) {
      debugIf(LOG, () -> "Terminating ZoolDataFlow: [" + zNode + "]");
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
  public void onNoChildren(final String zNode) {
    LOG.info("No Children Received for " + zNode + (dataSinkMap.containsKey(zNode) ? "(Watched!)" : "(Not Watching!)"));
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.stream()
            .filter(ZoolDataSink::isReadChildren)
            .forEach(handler -> handler.onNoChildren(zNode)));
  }

  @Override
  public void onChildren(final String zNode, final List<String> childNodes) {
    LOG.info("Children Received for " + zNode + " ==> " + childNodes.size() + (dataSinkMap.containsKey(zNode)
        ? "(Watched!)"
        : "(Not Watching!)"));
    // remove each of the data sinks that should disconnect when no data exists.
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.stream()
            .filter(ZoolDataSink::isReadChildren)
            .forEach(handler -> handler.onChildren(zNode, childNodes)));
  }

  @Override
  public void onDataNotExists(String zNode) {
    // remove each of the data sinks that should disconnect when no data exists.
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(handlers -> handlers.stream()
            .peek(handler -> handler.onDataNotExists(zNode))
            .filter(ZoolDataSink::willDisconnectOnNoData)
            .collect(Collectors.toList())
            .forEach(this::drainStop));
  }

  @Override
  public void onData(String zNode, byte[] data) {
    // remove each of the data sinks that should disconnect when no data exists.
    Optional.ofNullable(dataSinkMap.get(zNode))
        .ifPresent(dataSinkList -> dataSinkList.stream()
            .peek(dataSink -> dataSink.onData(dataSink.getZNode(), data))
            .filter(ZoolDataSink::willDisconnectOnData)
            .collect(Collectors.toList())
            .forEach(this::drainStop));
  }
}