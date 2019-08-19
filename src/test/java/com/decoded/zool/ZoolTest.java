package com.decoded.zool;


import com.decoded.zool.dataflow.DataFlowState;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.assertj.core.api.WithAssertions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;


public class ZoolTest implements WithAssertions {

  @BeforeClass
  public static void beforeClass() {
    BasicConfigurator.configure();
  }

  @Test
  public void testNoData() {
    MockData data = new MockData();
    Mocks mocks = new Mocks(data);
    Stubbing stubbing = new Stubbing(mocks, data);

    stubbing.stubZoolDataFlow(data.zNode, data.noBytes);
    mocks.zool.drain(mocks.dataSink);
    assertThat(mocks.dataFlow.getState()).isEqualTo(DataFlowState.CONNECTED);
    final byte[] byteData = mocks.zool.getData(data.zNode);

    assertThat(byteData).isEqualTo(data.noBytes);

    mocks.zool.drainStop(mocks.dataSink);
  }

  @Test
  public void testBasicDataFetch() throws Exception {
    MockData data = new MockData();
    Mocks mocks = new Mocks(data);
    Stubbing stubbing = new Stubbing(mocks, data);

    stubbing.stubZookeeperExists(data.zNode, true, data.defaultStat)
        .stubZookeeperExists(data.zNode, false, data.defaultStat)
        .stubZookeeperChildren(data.zNode, true, data.noChildren)
        .stubZookeeperChildren(data.zNode, false, data.noChildren)
        .stubZoolDataFlow(data.zNode, data.byteData);

    mocks.zool.drain(mocks.dataSink);
    assertThat(mocks.dataFlow.getState()).isEqualTo(DataFlowState.CONNECTED);
    final byte[] byteData = mocks.zool.getData(data.zNode);

    assertThat(byteData).isEqualTo(data.byteData);

    mocks.zool.drainStop(mocks.dataSink);
    verify(mocks.dataFlow, times(1)).get(eq(data.zNode));
  }

  @Test
  public void testDrainAndStop() throws Exception {
    MockData data = new MockData();
    Mocks mocks = new Mocks(data);
    Stubbing stubbing = new Stubbing(mocks, data);

    stubbing.stubZookeeperExists(data.zNode, true, data.defaultStat).stubZoolDataFlow(data.zNode, data.byteData);

    mocks.zool.drain(mocks.dataSink);
    assertThat(mocks.dataFlow.getState()).isEqualTo(DataFlowState.CONNECTED);
    mocks.zool.drainStop(mocks.dataSink);
    verify(mocks.dataFlow, times(1)).watch(eq(mocks.dataSink));
    verify(mocks.dataFlow, times(1)).unwatch(eq(data.zNode));
    verify(mocks.dataFlow, times(1)).drain(eq(mocks.dataSink));
    verify(mocks.dataFlow, times(1)).drainStop(eq(mocks.dataSink));
  }

  static class ZoolDataWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZoolDataWatcher.class);

    public void onData(String p, byte[] b) {
      LOG.info("onData: " + p + ", " + b.length);
    }

    public void onNoData(String p) {
      LOG.info("onNoData: " + p);
    }
  }

  /**
   * Test Mock Data
   */
  private static final class MockData {
    private boolean readChildren = true;
    private String zNode = "/zNode";
    private String zkHost = "localhost";
    private int zkPort = 2181;
    private int zkTimeout = 500;
    private byte[] byteData = {0x0, 0x1, 0x2};
    private byte[] noBytes = new byte[0];
    private Stat defaultStat = new Stat();
    private List<String> noChildren = Collections.emptyList();
    // this list is for just having some random children
    private List<String> someChildren = ImmutableList.of("manny", "moe", "jack");
    private KeeperException.ConnectionLossException connectionLossException
        = new KeeperException.ConnectionLossException();
  }

  /**
   * Stubbing Behaviors
   */
  private static final class Stubbing {
    private Mocks mocks;
    private MockData mockData;

    public Stubbing(Mocks mocks, MockData mockData) {
      this.mocks = mocks;
      this.mockData = mockData;
    }

    public Stubbing stubZookeeperExists(String path, boolean shouldWatch, Stat mockStat) throws Exception {
      when(mocks.zooKeeper.exists(path, shouldWatch)).thenReturn(mockStat);
      return this;
    }

    /**
     * Stub Child names to return from zk
     *
     * @param path       the path
     * @param childNames names of children to return
     *
     * @return Stubbing
     *
     * @throws Exception if zookeeper chokes
     */
    public Stubbing stubZookeeperChildren(String path, boolean watch, List<String> childNames) throws Exception {
      when(mocks.zooKeeper.getChildren(path, watch)).thenReturn(childNames);
      return this;
    }

    /**
     * Stub the Zool Data Flow, with some customizable inputs
     *
     * @param zNode           the node to stub
     * @param defaultByteData the data to return
     *
     * @return Stubbing
     */
    private Stubbing stubZoolDataFlow(String zNode, byte[] defaultByteData) {
      // use defaults from mockData
      mocks.dataFlow.setHost(mockData.zkHost).setPort(mockData.zkPort).setTimeout(mockData.zkTimeout);

      doReturn(defaultByteData).when(mocks.dataFlow).get(zNode);
      doReturn(zNode).when(mocks.dataSink).getZNode();
      doReturn(mocks.zooKeeper).when(mocks.dataFlow).createZookeeper();
      doReturn(mocks.dataBridge).when(mocks.dataFlow).createZoolDataBridge(mocks.dataSink);
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
      watcher = createBasicWatcher();
      dataSink = createBasicDataSink();
      zooKeeper = mock(ZooKeeper.class);
      when(zooKeeper.getState()).thenReturn(ZooKeeper.States.CONNECTED);
      executorService = mock(ExecutorService.class);
      dataFlow = createBasicDataFlow(executorService);
      dataBridge = createBasicDataBridge(zooKeeper, data.zNode, watcher, data.readChildren);
      zool = createBasicZoolClient(dataFlow);
    }

    /**
     * Alternate constructor to provide more custom mechanism
     *
     * @param dataSink        a custom data sink
     * @param executorService a custom executor service
     * @param dataFlow        a data flow.
     */
    public Mocks(ZoolDataSink dataSink, ExecutorService executorService, ZoolDataFlow dataFlow) {
      this.dataSink = dataSink;
      this.executorService = executorService;
      this.dataFlow = createBasicDataFlow(executorService);
      zool = createBasicZoolClient(dataFlow);
    }

    private Zool createBasicZoolClient(ZoolDataFlow dataFlow) {
      return spy(new TestZoolClient(dataFlow));
    }

    private ZoolDataBridge createBasicDataBridge(ZooKeeper zooKeeper,
        String zNode,
        ZoolWatcher zoolWatcher,
        boolean readChildren) {
      return spy(new ZoolDataBridgeImpl(zooKeeper, zNode, zoolWatcher, readChildren));
    }

    private ZoolDataFlowImpl createBasicDataFlow(ExecutorService executorService) {
      return spy(new ZoolDataFlowImpl(executorService));
    }

    private ZoolDataSink createBasicDataSink() {
      return spy(new ZoolDataSink() {
        private boolean readChildren;

        @Override
        public ZoolDataSink setReadChildren(final boolean readChildren) {
          this.readChildren = readChildren;
          return this;
        }

        @Override
        public ZoolDataSink setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler) {
          return this;
        }

        @Override
        public ZoolDataSink setNoChildNodesHandler(final Consumer<String> noChildNodesHandler) {
          return this;
        }

        @Override
        public boolean isReadChildren() {
          return readChildren;
        }

        @Override
        public void onChildren(final String zNode, final List<String> childNodes) {

        }

        @Override
        public void onNoChildren(final String zNode) {

        }

        @Override
        public String getZNode() {
          return "";
        }

        @Override
        public String getName() {
          return "";
        }

        @Override
        public void onData(String zNode, byte[] data) {
        }

        @Override
        public void onDataNotExists(String zNode) {
        }

        @Override
        public ZoolDataSink oneOff() {
          return this;
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
      });
    }

    private ZoolWatcher createBasicWatcher() {
      return spy(new ZoolWatcher() {
        private boolean readChildren;

        @Override
        public ZoolDataSink setReadChildren(final boolean readChildren) {
          this.readChildren = readChildren;
          return this;
        }

        @Override
        public boolean isReadChildren() {
          return this.readChildren;
        }

        @Override
        public ZoolDataSink setChildNodesHandler(final BiConsumer<String, List<String>> childNodesHandler) {
          return this;
        }

        @Override
        public ZoolDataSink setNoChildNodesHandler(final Consumer<String> noChildNodesHandler) {
          return this;
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
          return this;
        }

        @Override
        public ZoolDataSink disconnectWhenDataIsReceived() {
          return this;
        }

        @Override
        public ZoolDataSink oneOff() {
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
      });
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

  public static class Cfg {
    public int zkConnectTimeout = 10000;
    public String zkHost = "localhost";
    public int zkPort = 2181;
    public String zkServiceMapNode = "/servicemap";
  }

}
