package com.decoded.zool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public ZoolDataSinkImpl(String name, String zNode, BiConsumer<String, byte[]> dataHandler, Consumer<String> noDataHandler) {
    this(zNode, dataHandler, noDataHandler);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String getZNode() {
    return zNode;
  }

  @Override
  public void onData(String zNode, byte[] data) {
    if (!this.zNode.equals(zNode)) {
      throw new IllegalStateException("Cannot accept data from a path not designated by the watch path that this handler was constructed with!");
    }

    Optional.ofNullable(dataHandler)
        .ifPresent(dh -> dh.accept(zNode, data));
  }

  @Override
  public void onDataNotExists(String zNode) {
    Optional.ofNullable(noDataHandler)
        .ifPresent(ndh -> ndh.accept(zNode));
  }
}