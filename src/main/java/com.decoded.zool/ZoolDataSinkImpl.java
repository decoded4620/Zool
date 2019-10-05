package com.decoded.zool;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


/**
 * The {@link ZoolDataSink} accepts data from a {@link ZoolDataFlow} path watch. You can create any number of DataSink for a single
 * Data Flow. Also, you may drain data from different DataFlow on the same {@link Zool} instance.
 */
public class ZoolDataSinkImpl implements ZoolDataSink {

  private static final Logger LOG = LoggerFactory.getLogger(ZoolDataSinkImpl.class);
  private final String zNode;
  private final BiConsumer<String, byte[]> dataHandler;
  private final Consumer<String> noDataHandler;
  private BiConsumer<String, List<String>> childNodesHandler = (s, l) -> {
    LOG.debug("Default children nodes handler");
  };
  private Consumer<String> noChildNodesHandler = (s) -> {
    LOG.debug("Default no children handler");
  };

  private boolean disconnectingAfterLoadComplete = false;
  private boolean readChildren = false;
  private List<String> childNodeNames = Collections.emptyList();

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
  public boolean isDisconnectingAfterLoadComplete() {
    return disconnectingAfterLoadComplete;
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
    LOG.debug("No children found on " + zNode + "(has no children handler: " + (noChildNodesHandler != null) + ")");
    Optional.ofNullable(noChildNodesHandler).ifPresent(handler -> handler.accept(zNode));
  }

  @Override
  public void onChildren(final String zNode, final List<String> childNodes) {
    LOG.debug(
        "Children received for " + zNode + " => " + childNodes.size() + "(has children handler: " + (childNodesHandler != null) + ")");
    childNodeNames = ImmutableList.copyOf(childNodes);
    Optional.ofNullable(childNodesHandler).ifPresent(handler -> handler.accept(zNode, childNodeNames));
  }

  @Override
  public void onData(String zNode, byte[] data) {
    LOG.debug("Data received for " + zNode + ", " + data.length);

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
  public void onLoadComplete(final String zNode) {

  }

  public boolean isDisocnnectingAfterFirstLoad() {
    return disconnectingAfterLoadComplete;
  }

  @Override
  public ZoolDataSink disconnectAfterLoadComplete() {
    disconnectingAfterLoadComplete = true;
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ZoolDataSinkImpl that = (ZoolDataSinkImpl) o;

    return new EqualsBuilder().append(disconnectingAfterLoadComplete, that.disconnectingAfterLoadComplete)
        .append(readChildren, that.readChildren)
        .append(zNode, that.zNode)
        .append(dataHandler, that.dataHandler)
        .append(noDataHandler, that.noDataHandler)
        .append(childNodesHandler, that.childNodesHandler)
        .append(noChildNodesHandler, that.noChildNodesHandler)
        .append(childNodeNames, that.childNodeNames)
        .append(name, that.name)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(zNode)
        .append(dataHandler)
        .append(noDataHandler)
        .append(childNodesHandler)
        .append(noChildNodesHandler)
        .append(disconnectingAfterLoadComplete)
        .append(readChildren)
        .append(childNodeNames)
        .append(name)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("zNode", zNode)
        .append("dataHandler", dataHandler)
        .append("noDataHandler", noDataHandler)
        .append("childNodesHandler", childNodesHandler)
        .append("noChildNodesHandler", noChildNodesHandler)
        .append("disconnectingAfterLoadComplete", disconnectingAfterLoadComplete)
        .append("readChildren", readChildren)
        .append("childNodeNames", childNodeNames)
        .append("name", name)
        .toString();
  }
}