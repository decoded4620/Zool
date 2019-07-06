package com.decoded.zool;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


/**
 * Interface for an accepter of Zookeeper node data.
 */
public interface ZoolDataSink {
  /**
   * Set the handler for child node information
   *
   * @param childNodesHandler the handler for children nodes.
   *
   * @return this data sink.
   */
  ZoolDataSink setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler);

  /**
   * Set the handler for not having child node information on a path
   *
   * @param noChildNodesHandler the handler for no children nodes.
   *
   * @return this data sink.
   */
  ZoolDataSink setNoChildNodesHandler(final Consumer<String> noChildNodesHandler);

  /**
   * Read Children Nodes and watch for result updates
   *
   * @return true if this data sink also wants to watch for child updates.
   */
  boolean isReadChildren();

  /**
   * Read Children Nodes and watch for result updates
   *
   * @param readChildren a flag.
   *
   * @return this data sink
   */
  ZoolDataSink setReadChildren(final boolean readChildren);

  /**
   * Named datasink (useful for debugging and visualizing the flow of zookeeper data to your application).
   *
   * @return a String.
   */
  String getName();

  /**
   * The Zookeeper node name that is draining to this data sink.
   *
   * @return a String.
   */
  String getZNode();

  /**
   * When no children are found on the path
   *
   * @param zNode the path.
   */
  void onNoChildren(String zNode);

  /**
   * When reading children, this will handle child node name updates.
   *
   * @param zNode      the patch that received child nodes
   * @param childNodes the child node names
   */
  void onChildren(String zNode, List<String> childNodes);

  /**
   * The main drain method. Data recieved on the path will be directed here.
   *
   * @param zNode the event path (as part of zookeepers structure, this is passed in). It must be equal to the path that
   *              was assigned upon creation of the data sink, or it should not be handled by this sink.
   * @param data  the incoming data.
   */
  void onData(String zNode, byte[] data);

  /**
   * Invoked when there is no data at the specified zNode.
   *
   * @param zNode the node path.
   */
  void onDataNotExists(String zNode);


  /**
   * If true, this data sink should be disconnected after it receives and processes data.
   *
   * @return a boolean
   */
  boolean willDisconnectOnData();

  /**
   * If true, this data sink should be discarded if it recieves no data.
   *
   * @return a boolean
   */
  boolean willDisconnectOnNoData();

  /**
   * Calling this on a data sink (before fetching data) will cause the data sink to disconnect when its onDataNotExists
   * handler is called.
   * <p>
   * This is handy for cleaning up as early as possible
   *
   * @return this data sink.
   */
  ZoolDataSink disconnectWhenNoDataExists();

  /**
   * Calling this on data sink (before fetching data) will cause the data sink to disconnect when its onData handler is
   * called.
   *
   * @return this data sink.
   */
  ZoolDataSink disconnectWhenDataIsReceived();

  /**
   * Calling this on data sink has the same behavior as calling both disconnectWhenDataIsReceived and
   * disconnectWhenNoDataExists together.
   *
   * @return this data sink
   */
  ZoolDataSink oneOff();
}