package com.decoded.zool;

/**
 * Interface for an accepter of Zookeeper node data.
 */
public interface ZoolDataSink {
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
   * The main drain method. Data recieved on the path will be directed here.
   *
   * @param zNode the event path (as part of zookeepers structure, this is passed in). It must be equal to the path
   *              that was assigned upon creation of the data sink, or it should not be handled by this sink.
   * @param data  the incoming data.
   */
  void onData(String zNode, byte[] data);

  /**
   * Invoked when there is no data at the specified zNode.
   *
   * @param zNode the node path.
   */
  void onDataNotExists(String zNode);
}