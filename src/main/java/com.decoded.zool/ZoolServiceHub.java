package com.decoded.zool;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


public class ZoolServiceHub {
  private static final String HTTP_PORT_PROP = "http.port";

  private static final Logger LOG = LoggerFactory.getLogger(ZoolServiceHub.class);
  private final Zool zoolClient;
  private final ExecutorService executorService;
  private String serviceKey = "milliContainerService";
  private int pollingInterval = 10000;
  private boolean isProd = false;
  private Map<String, Set<String>> millServiceMap = new ConcurrentHashMap<>();
  private int port = -1;

  @Inject
  public ZoolServiceHub(Zool zool, ExecutorService executorService) {
    this.zoolClient = zool;
    this.executorService = executorService;
  }

  /**
   * Set the host service key for this ZoolServiceHub
   *
   * @param serviceKey the service key
   */
  public void setServiceKey(String serviceKey) {
    this.serviceKey = serviceKey;
  }

  public void setPollingInterval(int pollingInterval) {
    this.pollingInterval = pollingInterval;
  }

  public void setProd(boolean prod) {
    isProd = prod;
  }

  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Gets a set of known services from zookeeper.
   *
   * @return a Set of string service names.
   */
  public List<String> getKnownServices() {
    return new ArrayList<>(millServiceMap.keySet());
  }

  /**
   * Returns the set of hosts for a service.
   *
   * @param serviceKey the zookeeper service key
   * @return A set of hosts for a specific service.
   */
  public List<String> getHostsForService(String serviceKey) {
    return new ArrayList<>(millServiceMap.computeIfAbsent(serviceKey, sk -> new HashSet<>()));
  }

  public void start() {
    final String serviceMap = this.zoolClient.getServiceMapNode();
    final String gateway = this.zoolClient.getGatewayMapNode();

    List<ZoolDataSink> dataHandlers = ImmutableList.of(
        new ZoolDataSinkImpl(gateway, this::onGatewayData, this::onGatewayNoData),
        new ZoolDataSinkImpl(serviceMap, this::onZoolServiceMapData, this::onZoolServiceMapNoData));

    dataHandlers.forEach(this.zoolClient::drain);

    this.zoolClient.connect();
  }

  public void stop() {
    this.zoolClient.disconnect();
  }


  private void onGatewayData(String path, byte[] data) {
    LOG.debug("onGatewayData: " + path + ", " + data.length + " bytes, value: \n" + new String(data));
  }

  private void onGatewayNoData(String path) {
    LOG.error("onGatewayNoData: " + path);
    ZoolUtil.createEmptyPersistentNode(zoolClient, path);
  }

  private void onZoolServiceMapData(String path, byte[] data) {
    LOG.debug("onZookeeperServiceMapData: " + path + ", " + data.length + " bytes, value: \n" + new String(data));

    List<String> children = zoolClient.getChildren(path);

    LOG.debug("Total Known Services: " + children.size());
    children.forEach(childName -> {
      final String servicesNodeName = path + '/' + childName;
      LOG.debug("Storing service host list for: " + childName);
      millServiceMap.computeIfAbsent(servicesNodeName, p -> new HashSet<>());
      collectNodeChildren(getPubDns(), servicesNodeName);
    });

    // watch for our service node
    this.zoolClient.drain(
        new ZoolDataSinkImpl(this.zoolClient.getServiceMapNode() + '/' + serviceKey, this::onServiceNodeData,
                             this::onServiceNodeNoData));
  }

  private void onZoolServiceMapNoData(String path) {
    LOG.error("onZoolServiceMapNoData: " + path);
    ZoolUtil.createEmptyPersistentNode(zoolClient, path);
  }

  private void onZoolHostNodeData(String path, byte[] data) {
    LOG.info("onZoolHostNodeData: " + path + ", " + data.length + " bytes");
  }

  private void onZoolHostNodeNoData(String path) {
    LOG.warn("onZoolHostNodeNoData: " + path);
  }

  private void onServiceNodeData(String serviceNodePath, byte[] data) {
    NetworkUtil.getMaybeCanonicalLocalhostName().ifPresent(localhostUrl -> {
      LOG.info("Booting up a " + (isProd ? "Production" : "Dev") + " host@" + localhostUrl);
      this.bootServicesHub(serviceNodePath, localhostUrl);
    });
  }

  /**
   * Collects the children of a specific service node to store the dns.
   *
   * @param pubDns          the public dns of this server
   * @param serviceNodePath the service node path
   */
  private void collectNodeChildren(String pubDns, String serviceNodePath) {
    List<String> children = zoolClient.getChildren(serviceNodePath);

    // clear the service map
    millServiceMap.computeIfAbsent(serviceNodePath, p -> new HashSet<>());
    millServiceMap.get(serviceNodePath).clear();

    final CountDownLatch latch = new CountDownLatch(children.size());
    children.forEach(childName -> {
      zoolClient.getZookeeper().getData(serviceNodePath + '/' + childName, false, (rc, p, ctx, d, s) -> {
        millServiceMap.get(serviceNodePath).add(new String(d));
        latch.countDown();
      }, null);
    });

    try {
      latch.await(zoolClient.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      throw new RuntimeException("Timed out connecting to Zookeeper", ex);
    }

    if (serviceNodePath.endsWith(serviceKey)) {
      List<String> myHosts = new ArrayList<>(millServiceMap.computeIfAbsent(serviceNodePath, p -> new HashSet<>()));
      if (!myHosts.contains(pubDns)) {
        myHosts.add(pubDns);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Milli Service Nodes are Loaded: " + millServiceMap.size() + " services");
      millServiceMap.forEach((serviceName, hosts) -> {
        LOG.debug("Service: " + serviceName + ", hosts: ");
        hosts.forEach(hostName -> LOG.debug("\t - " + hostName));
      });
    }

    // keep polling
    executorService.submit(() -> this.poll(pubDns, serviceNodePath));
  }

  /**
   * Called internally by the thread submitted to executor service.
   *
   * @param pubDns          the public dns.
   * @param serviceNodePath the service instance path
   */
  private void poll(String pubDns, String serviceNodePath) {
    if (zoolClient.isConnected()) {
      try {
        Thread.sleep(pollingInterval);
        this.collectNodeChildren(pubDns, serviceNodePath);
      } catch (InterruptedException ex) {
        zoolClient.disconnect();
        LOG.error("Disconnected from Zookeeper", ex);
        throw new RuntimeException("Disconnected", ex);
      }
    }
  }

  private int getCurrentPort() {
    int defaultPort = port;
    int currentPort;
    try {
      currentPort = Integer.valueOf(System.getProperties().getProperty(HTTP_PORT_PROP));
    } catch (NumberFormatException ex) {
      LOG.warn("Falling back to default port: " + defaultPort);
      currentPort = defaultPort;
    }

    return currentPort;
  }

  private String getPubDns() {
    // You must set the environment variable denoted in DeployConstants
    // for both dev and prod PUB DNS to run the container application.
    return System.getenv(
        isProd ? DeployConstants.ENV_PUB_DNS : DeployConstants.ENV_DEV_PUB_DNS) + ':' + getCurrentPort();
  }

  /**
   * Boots the hub with a service node path, and localhost url.
   *
   * @param serviceNodePath service node path
   * @param localhostUrl    the localhost url.
   */
  private void bootServicesHub(final String serviceNodePath, final String localhostUrl) {
    String pubDns = getPubDns();

    collectNodeChildren(pubDns, serviceNodePath);
    final String instancePath = serviceNodePath + '/' + localhostUrl + ':' + getCurrentPort();

    // watch for service map node changes
    // when new services come online, or die
    this.zoolClient.drain(new ZoolDataSinkImpl(instancePath, this::onZoolHostNodeData, this::onZoolHostNodeNoData));
    ZoolUtil.createEphemeralNode(zoolClient, instancePath, pubDns.getBytes());
  }

  /**
   * Called when there is no node data int he service instance node.
   *
   * @param path the service path
   */
  private void onServiceNodeNoData(String path) {
    ZoolUtil.createPersistentNode(zoolClient, path, getPubDns().getBytes());
  }
}
