package com.decoded.zool;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


/**
 * The {@link ZoolDataSink} accepts data from a Zookeeper path watch. You can create any number of DataSink for a single
 * node. Also, you may drain data from different nodes on the same {@link Zool} instance.
 */
public class ZoolDataSinkImpl implements ZoolDataSink {

  private static final Logger LOG = LoggerFactory.getLogger(ZoolDataSinkImpl.class);
  private final String zNode;
  private final BiConsumer<String, byte[]> dataHandler;
  private final Consumer<String> noDataHandler;
  private BiConsumer<String, List<String>> childNodesHandler;
  private Consumer<String> noChildNodesHandler;

  private boolean willDisconnectOnNoData;
  private boolean willDisconnectOnData;
  private boolean readChildren;
  private List<String> childNodeNames;

  private String name = ZoolDataSink.class.getName();

  /**
   * Constructor
   *
   * @param zNode         the zookeeper node
   * @param dataHandler   a {@link BiConsumer} which accepts the zNode and data
   * @param noDataHandler a {@link Consumer} which accepts zNode path
   */
  public ZoolDataSinkImpl(String zNode, BiConsumer<String, byte[]> dataHandler, Consumer<String> noDataHandler) {
    this.zNode = zNode;
    this.dataHandler = dataHandler;
    this.noDataHandler = noDataHandler;
    this.name += zNode;
    if (zNode == null || zNode.isEmpty()) {
      throw new IllegalArgumentException("Watch Path cannot be null or empty!");
    }
  }

  /**
   * Named DataSink
   *
   * @param name          the name you wish to assign. The default name is ZoolDataSink.
   * @param zNode         the node to watch
   * @param dataHandler   a {@link BiConsumer} which accepts the zNode and data
   * @param noDataHandler a {@link Consumer} which accepts zNode path
   */
  public ZoolDataSinkImpl(String name,
      String zNode,
      BiConsumer<String, byte[]> dataHandler,
      Consumer<String> noDataHandler) {
    this(zNode, dataHandler, noDataHandler);
    this.name = name;
  }

  @Override
  public boolean isReadChildren() {
    return readChildren;
  }

  @Override
  public ZoolDataSinkImpl setReadChildren(final boolean readChildren) {
    this.readChildren = readChildren;
    return this;
  }

  @Override
  public ZoolDataSinkImpl setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler) {
    this.childNodesHandler = childNodesHandler;
    return this;
  }

  @Override
  public ZoolDataSinkImpl setNoChildNodesHandler(final Consumer<String> noChildNodesHandler) {
    this.noChildNodesHandler = noChildNodesHandler;
    return this;
  }

  @Override
  public boolean willDisconnectOnData() {
    return willDisconnectOnData;
  }

  @Override
  public boolean willDisconnectOnNoData() {
    return willDisconnectOnNoData;
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
  public void onNoChildren(final String zNode) {
    LOG.info("No children found on " + zNode + "(has no children handler: " + (noChildNodesHandler != null) + ")");
    Optional.ofNullable(noChildNodesHandler).ifPresent(handler -> handler.accept(zNode));
  }

  @Override
  public void onChildren(final String zNode, final List<String> childNodes) {
    LOG.info(
        "Children received for " + zNode + " => " + childNodes.size() + "(has children handler: " + (childNodesHandler != null) + ")");
    childNodeNames = ImmutableList.copyOf(childNodes);
    Optional.ofNullable(childNodesHandler).ifPresent(handler -> handler.accept(zNode, childNodeNames));
  }

  @Override
  public void onData(String zNode, byte[] data) {
    LOG.info("Data received for " + zNode + ", " + data.length);

    if (!this.zNode.equals(zNode)) {
      throw new IllegalStateException(
          "Cannot accept data from a path not designated by the watch path that this handler was constructed with!");
    }

    Optional.ofNullable(dataHandler).ifPresent(dh -> dh.accept(zNode, data));
  }

  @Override
  public void onDataNotExists(String zNode) {
    Optional.ofNullable(noDataHandler).ifPresent(ndh -> ndh.accept(zNode));
  }

  @Override
  public ZoolDataSink disconnectWhenDataIsReceived() {
    willDisconnectOnData = true;
    return this;
  }

  @Override
  public ZoolDataSink disconnectWhenNoDataExists() {
    willDisconnectOnNoData = true;
    return this;
  }

  @Override
  public ZoolDataSink oneOff() {
    LOG.info(
        "Creating a One Off DataSink: " + getName() + "(" + getZNode() + "), which disconnects after its interaction "
            + "completes");
    disconnectWhenDataIsReceived();
    disconnectWhenNoDataExists();
    return this;
  }
}