package com.decoded.zool;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.decoded.zool.ZoolLoggingUtil.debugIf;


/**
 * Reads Data from Zool
 */
public class ZoolReader {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolReader.class);

  private Zool zool;
  private Map<String, List<ZoolDataSink>> channels = new ConcurrentHashMap<>();

  @Inject
  public ZoolReader(Zool zool) {
    this.zool = zool;
  }

  public Zool getZool() {
    return zool;
  }

  /**
   * Read a Zool Channel, using the drain method
   *
   * @param path        the channel path to read.
   * @param watch       true to continue to receive drain updates
   * @param dataHandler a data drain handler
   * @param ctx         the context
   *
   * @return this {@link ZoolReader}
   */
  public ZoolReader readChannel(String path, boolean watch, BiConsumer<String, byte[]> dataHandler, Object ctx) {
    debugIf(LOG, () -> "Read Zool Channel: " + path + ", watch: " + watch);
    zool.drain(path, watch, dataHandler, ctx);
    return this;
  }

  /**
   * True if zool reader is reading a specific channel. Each reader can read multiple channels, each channel can have
   * multiple {@link ZoolDataSink} attached.
   *
   * @param channel a channel (zk node)
   *
   * @return true if we are reading the channel.
   */
  public boolean isReading(String channel) {
    return channels.containsKey(channel);
  }

  /**
   * Read a zool channel Creates a new {@link ZoolDataSink} against a specific channel.
   *
   * @param channel       the path to the node
   * @param dataHandler   the handler when data is received
   * @param noDataHandler the handler when no data was
   *
   * @return this {@link ZoolReader}
   */
  public ZoolDataSink readChannel(String channel,
      BiConsumer<String, byte[]> dataHandler,
      Consumer<String> noDataHandler) {
    debugIf(LOG, () -> "Read Zool Channel: " + channel);
    ZoolDataSink sink = new ZoolDataSinkImpl(channel, dataHandler, noDataHandler);
    channels.computeIfAbsent(channel, c -> new ArrayList<>()).add(sink);
    zool.drain(sink);
    return sink;
  }

  /**
   * Read the channel and child nodes for a parent node.
   *
   * @param channel           the channel (node path)
   * @param dataHandler       the handler for data existence
   * @param noDataHandler     the handler for no data existence
   * @param childrenHandler   the handler for children
   * @param noChildrenHandler the handler for no children
   *
   * @return a {@link ZoolDataSink} that will have the child and channel data drained to it.
   */
  public ZoolDataSink readChannelAndChildren(String channel,
      BiConsumer<String, byte[]> dataHandler,
      Consumer<String> noDataHandler,
      BiConsumer<String, List<String>> childrenHandler,
      Consumer<String> noChildrenHandler) {

    ZoolDataSink sink = new ZoolDataSinkImpl(channel, dataHandler, noDataHandler);
    channels.computeIfAbsent(channel, c -> new ArrayList<>()).add(sink);

    sink.setReadChildren(true).setChildNodesHandler(childrenHandler).setNoChildNodesHandler(noChildrenHandler);

    debugIf(LOG, () -> "Read Zool Channel Data and Children: " + channel);

    zool.drain(sink);
    return sink;
  }


  /**
   * Read Children only (don't care about the data in a node)
   *
   * @param channel           the channel to read
   * @param childrenHandler   the handler for children
   * @param noChildrenHandler the no children handler
   *
   * @return a new data sink.
   */
  public ZoolDataSink readChildren(String channel,
      BiConsumer<String, List<String>> childrenHandler,
      Consumer<String> noChildrenHandler) {
    ZoolDataSink sink = new ZoolDataSinkImpl(channel, null, null).setReadChildren(true)
        .setChildNodesHandler(childrenHandler)
        .setNoChildNodesHandler(noChildrenHandler);

    channels.computeIfAbsent(channel, c -> new ArrayList<>()).add(sink);

    debugIf(LOG, () -> "Read Zool Channel Children Only: " + channel);

    zool.drain(sink);
    return sink;
  }


  /**
   * Returns the set of channels we are currently listening on
   *
   * @return a List of {@link ZoolDataSink}
   */
  public Map<String, List<ZoolDataSink>> getChannels() {
    return ImmutableMap.copyOf(channels);
  }

  /**
   * True if the node at path already exists
   *
   * @param path the path
   *
   * @return a boolean
   */
  public boolean nodeExists(String path) {
    return zool.nodeExists(path);
  }

  /**
   * Get the children of a node
   *
   * @param nodePath the patch
   * @param watch    true if you want zookeeper to continue to update any data sinks (channels) on that path. if no Data
   *                 sink is connected, this has no logical effect, but may incur connection resource cost.
   *
   * @return list of child names.
   */
  public List<String> getChildNodes(String nodePath, boolean watch) {
    debugIf(LOG, () -> "Get child nodes under: " + nodePath + ", keeping watch: " + watch);
    if (watch && !channels.containsKey(nodePath)) {
      LOG.warn(
          "There are no Zool Data Sink objects listening to channel: " + nodePath + ", so updates will not be " +
              "processed by this Zool Reader");
    }
    return zool.getChildren(nodePath, watch);
  }
}
