package com.decoded.zool;


import com.google.inject.Inject;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

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

    verify(mocks.dataFlow, times(1)).watch(eq(data.zNode));
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
      when(mocks.dataFlow.createDataBridge(mockData.zNode)).thenReturn(mocks.dataBridge);
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
        public String getName() {
          return "There is no dana, only Zool (Zuul)";
        }

        @Override
        public String getZNode() {
          return "Zuul";
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
      });

      dataSink = spy(new ZoolDataSink() {
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

      setHost(cfg.zkHost);
      setPort(cfg.zkPort);
      setTimeout(cfg.zkConnectTimeout);
      setServiceMapNode(cfg.zkServiceMapNode);
      setGatewayMapNode(cfg.zkGatewayNode);
    }

  }

  public static final class Cfg {
    public int zkConnectTimeout = 10000;
    public String zkHost = "localhost";
    public int zkPort = 2181;
    public String zkServiceMapNode = "/servicemap";
    public String zkGatewayNode = "/gateway";
  }

}
