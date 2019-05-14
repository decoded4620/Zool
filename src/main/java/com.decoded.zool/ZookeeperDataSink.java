package com.decoded.zool;

public interface ZookeeperDataSink {
  String getPath();

  void onData(String path, byte[] data);

  void onDataNotExists(String path);
}