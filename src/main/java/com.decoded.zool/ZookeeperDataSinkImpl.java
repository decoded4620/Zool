package com.decoded.zool;

import play.Logger;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Use this as a handler for ZooKeeper data.
 */
public class ZookeeperDataSinkImpl implements ZookeeperDataSink {

  private final String watchPath;
  private static final Logger.ALogger LOG = Logger.of(ZookeeperDataSinkImpl.class);
  private final BiConsumer<String, byte[]> dataHandler;
  private final Consumer<String> noDataHandler;

  public ZookeeperDataSinkImpl(String watchPath, BiConsumer<String, byte[]> dataHandler, Consumer<String> noDataHandler) {
    this.watchPath = watchPath;
    this.dataHandler = dataHandler;
    this.noDataHandler = noDataHandler;

    if (watchPath == null || watchPath.isEmpty()) {
      throw new IllegalArgumentException("Watch Path cannot be null or empty!");
    }
    LOG.warn("ZookeeperDataSinkImpl constructed for: " + watchPath);
  }

  @Override
  public String getPath() {
    return watchPath;
  }

  @Override
  public void onData(String path, byte[] data) {
    if (!watchPath.equals(path)) {
      throw new IllegalStateException("Cannot accept data from a path not designated by the watch path that this handler was constructed with!");
    }

    LOG.info("onData(): " + path + ", " + data.length + " bytes");
    Optional.ofNullable(dataHandler)
        .ifPresent(dh -> dh.accept(path, data));
  }

  @Override
  public void onDataNotExists(String path) {
    LOG.warn("onDataNotExists() -> " + path);
    Optional.ofNullable(noDataHandler)
        .ifPresent(ndh -> ndh.accept(path));
  }
}