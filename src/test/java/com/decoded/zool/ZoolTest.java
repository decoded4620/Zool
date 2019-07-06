package com.decoded.zool;


import com.google.inject.Inject;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

public class ZoolTest {

  @Test
  public void testAddRemoveDataSink() {
    MockData data = new MockData();
    Mocks mocks = new Mocks(data);
    Stubbing stubbing = new Stubbing(mocks, data);

    // stub the included data flow (from mocks)
    stubbing.stubZoolDataFlow();

    mocks.zool.drain(mocks.dataSink);
    mocks.zool.drainStop(mocks.dataSink);

    verify(mocks.dataFlow, times(1)).watch(eq(mocks.dataSink));
    verify(mocks.dataFlow, times(1)).unwatch(eq(data.zNode));
    verify(mocks.dataFlow, times(1)).drain(eq(mocks.dataSink));
    verify(mocks.dataFlow, times(1)).drainStop(eq(mocks.dataSink));
  }

  private static final class MockData {
    private String zNode = "/zNode";
    private String zkHost = "localhost";
    private int zkPort = 2181;
    private int zkTimeout = 500;
  }

  private static final class Stubbing {
    private Mocks mocks;
    private MockData mockData;

    public Stubbing(Mocks mocks, MockData mockData) {
      this.mocks = mocks;
      this.mockData = mockData;
    }

    private Stubbing stubZoolDataFlow() {
      when(mocks.dataFlow.createZookeeper()).thenReturn(mocks.zooKeeper);
      when(mocks.dataFlow.createZoolDataBridge(mockData.zNode)).thenReturn(mocks.dataBridge);
      when(mocks.dataSink.getZNode()).thenReturn(mockData.zNode);

      mocks.dataFlow.setHost(mockData.zkHost)
          .setPort(mockData.zkPort)
          .setTimeout(mockData.zkTimeout);
      return this;
    }
  }


  private static final class Mocks {
    private ZooKeeper zooKeeper;
    private ZoolDataFlowImpl dataFlow;
    private Zool zool;
    private ZoolWatcher watcher;
    private ExecutorService executorService;
    private ZoolDataBridge dataBridge;
    private ZoolDataSink dataSink;

    public Mocks(MockData data) {
      watcher = spy(new ZoolWatcher() {
        @Override
        public ZoolDataSinkImpl setReadChildren(final boolean readChildren) {
          return null;
        }

        @Override
        public boolean isReadChildren() {
          return false;
        }

        @Override
        public ZoolDataSink setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler) {
          return null;
        }

        @Override
        public ZoolDataSink setNoChildNodesHandler(final Consumer<String> noChildNodesHandler) {
          return null;
        }

        @Override
        public String getName() {
          return "There is no dana, only Zool (Zuul)";
        }

        @Override
        public String getZNode() {
          return "Zuul";
        }

        @Override
        public void onChildren(final String zNode, final List<String> childNodes) {

        }

        @Override
        public void onNoChildren(final String zNode) {

        }

        @Override
        public void onData(String zNode, byte[] data) {

        }

        @Override
        public void onDataNotExists(String zNode) {

        }

        @Override
        public void onZoolSessionInvalid(KeeperException.Code rc, String nodePath) {
        }

        @Override
        public ZoolDataSink disconnectWhenNoDataExists() {
          return null;
        }

        @Override
        public ZoolDataSink disconnectWhenDataIsReceived() {
          return null;
        }

        @Override
        public ZoolDataSink oneOff() {
          return null;
        }

        @Override
        public boolean willDisconnectOnData() {
          return false;
        }

        @Override
        public boolean willDisconnectOnNoData() {
          return false;
        }
      });

      dataSink = spy(new ZoolDataSink() {
        @Override
        public ZoolDataSinkImpl setReadChildren(final boolean readChildren) {
          return null;
        }

        @Override
        public ZoolDataSink setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler) {
          return null;
        }

        @Override
        public ZoolDataSink setNoChildNodesHandler(final Consumer<String> noChildNodesHandler) {
          return null;
        }

        @Override
        public boolean isReadChildren() {
          return false;
        }

        @Override
        public void onChildren(final String zNode, final List<String> childNodes) {

        }

        @Override
        public void onNoChildren(final String zNode) {

        }

        @Override
        public String getZNode() {
          return null;
        }

        @Override
        public String getName() {
          return null;
        }

        @Override
        public void onData(String zNode, byte[] data) {
        }

        @Override
        public void onDataNotExists(String zNode) {
        }

        @Override
        public ZoolDataSink oneOff() {
          return null;
        }

        @Override
        public ZoolDataSink disconnectWhenDataIsReceived() {
          return null;
        }

        @Override
        public ZoolDataSink disconnectWhenNoDataExists() {
          return null;
        }

        @Override
        public boolean willDisconnectOnData() {
          return false;
        }

        @Override
        public boolean willDisconnectOnNoData() {
          return false;
        }
      });

      zooKeeper = mock(ZooKeeper.class);

      dataBridge = spy(new ZoolDataBridgeImpl(zooKeeper,
          data.zNode, watcher));

      executorService = mock(ExecutorService.class);
      dataFlow = spy(new ZoolDataFlowImpl(executorService));
      zool = spy(new TestZoolClient(dataFlow));
    }
  }

  public static class TestZoolClient extends Zool {
    private final Cfg cfg;

    @Inject
    public TestZoolClient(ZoolDataFlow zoolDataFlow) {
      super(zoolDataFlow);
      cfg = new Cfg();

      setZookeeperHost(cfg.zkHost);
      setPort(cfg.zkPort);
      setTimeout(cfg.zkConnectTimeout);
      setServiceMapNode(cfg.zkServiceMapNode);
    }

  }

  public static final class Cfg {
    public int zkConnectTimeout = 10000;
    public String zkHost = "localhost";
    public int zkPort = 2181;
    public String zkServiceMapNode = "/servicemap";
  }

}
