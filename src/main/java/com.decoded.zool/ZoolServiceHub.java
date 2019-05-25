package com.decoded.zool;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NoRouteToHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.decoded.zool.ZoolUtil.debugIf;

/**
 * This class wraps Zool and interacts with it as a Service announcement portal.
 * Each Application Container that uses a ZoolServiceHub can define a service key
 * for every container with the same service key, each container will be grouped into a list
 * of hosts for that service key. Each host on the Zool Cluster will get a copy of the entire
 * services map. Which is how each host knows about all other services, and their locations inherently.
 */
public class ZoolServiceHub {
  private static final String HTTP_PORT_PROP = "http.port";

  private boolean firstLoad = true;
  private static final Logger LOG = LoggerFactory.getLogger(ZoolServiceHub.class);
  private final Zool zoolClient;
  private final ExecutorService executorService;
  private String serviceKey = "milliContainerService";
  private int pollingInterval = 10000;
  private boolean isProd = false;
  private Map<String, Set<String>> millServiceMap = new ConcurrentHashMap<>();
  private int port = -1;

  private Runnable gatewayCallback;
  private Runnable serviceNodeCallback;
  private Runnable serviceMapCallback;
  private Runnable hostsLoadedCallback;
  private Runnable hostUpdatedCallback;

  @Inject
  public ZoolServiceHub(Zool zool, ExecutorService executorService) {
    this.zoolClient = zool;
    this.executorService = executorService;
  }

  public ZoolServiceHub setServiceMapCallback(Runnable serviceMapCallback) {
    this.serviceMapCallback = serviceMapCallback;
    return this;
  }

  public ZoolServiceHub setServiceNodeCallback(Runnable serviceNodeCallback) {
    this.serviceNodeCallback = serviceNodeCallback;
    return this;
  }

  public ZoolServiceHub setGatewayCallback(Runnable gatewayCallback) {
    this.gatewayCallback = gatewayCallback;
    return this;
  }

  public ZoolServiceHub setHostsLoadedCallback(Runnable hostsLoadedCallback) {
    this.hostsLoadedCallback = hostsLoadedCallback;
    return this;
  }

  public ZoolServiceHub setHostUpdatedCallback(Runnable hostUpdatedCallback) {
    this.hostUpdatedCallback = hostUpdatedCallback;
    return this;
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

  // TODO
  public String getGatewayHost(String serviceKey) {
    // returns the gateway representation of the service key.
    final String gatwayMapNode = this.zoolClient.getGatewayMapNode();
    debugIf(() -> "getNextHost[" + serviceKey + "]");
    List<String> hosts = getHostsForService(gatwayMapNode + '/' + serviceKey);
    if(hosts.isEmpty()) {
      LOG.error("Service key " + serviceKey + " was invalid. Valid service keys: ");
      getKnownServices().forEach(knownService -> {
        LOG.warn(knownService);
        getHostsForService(knownService).forEach(host -> LOG.warn("\t" + host));
      });

      return "";
    }
    final int idx = Math.max(0, new Random().nextInt()) % hosts.size();
    return hosts.get(idx);

  }
  /**
   * Returns the next "best" host to use for a service key.
   * @param serviceKey the service key.
   * @return String the next host.
   */
  public String getNextHost(String serviceKey) {
    // clients should not have to supply the service map node
    final String serviceMap = this.zoolClient.getServiceMapNode();
    final String path = serviceMap + '/' + serviceKey;
    debugIf(() -> "getNextHost[" + path + "]");
    List<String> hosts = getHostsForService(path);
    if(hosts.isEmpty()) {
      LOG.error("Service path " + path + " was invalid. Valid service keys: ");
      getKnownServices().forEach(knownService -> getHostsForService(knownService).forEach(
          host -> LOG.warn("\t" + serviceMap + '/' + knownService + "::" + host)));
      throw new RuntimeException(new NoRouteToHostException("Service Key " + serviceKey + " has no hosts"));
    }

    final int idx = Math.max(0, new Random().nextInt()) % hosts.size();
    debugIf(() -> "Getting " + serviceKey + " host at " + idx);
    return hosts.get(idx);
  }
  /**
   * Start It
   */
  public void start() {
    debugIf(() -> "starting up...");
    final String serviceMap = this.zoolClient.getServiceMapNode();
    final String gateway = this.zoolClient.getGatewayMapNode();

    List<ZoolDataSink> dataHandlers = ImmutableList.of(
        new ZoolDataSinkImpl(gateway, this::onGatewayData, this::onGatewayNoData),
        new ZoolDataSinkImpl(serviceMap, this::onZoolServiceMapData, this::onZoolServiceMapNoData));

    dataHandlers.forEach(this.zoolClient::drain);

    this.zoolClient.connect();
  }

  /**
   * Stop it
   */
  public void stop() {
    this.zoolClient.disconnect();
  }


  private void onGatewayData(String path, byte[] data) {
    ZoolUtil.debugIf(() -> "onGatewayData: " + path + ", " + data.length + " bytes, value: \n" + new String(data));
    Optional.ofNullable(this.gatewayCallback).ifPresent(Runnable::run);
  }

  private void onGatewayNoData(String path) {
    LOG.info("onGatewayNoData: " + path);
    ZoolUtil.createEmptyPersistentNode(zoolClient, path);
  }

  private void onZoolServiceMapData(String path, byte[] data) {
    ZoolUtil.debugIf(() -> "onZookeeperServiceMapData: " + path + ", " + data.length + " bytes, value: \n" + new String(data));

    List<String> children = zoolClient.getChildren(path);

    ZoolUtil.debugIf(() -> "Total Known Services: " + children.size());
    children.forEach(childName -> {
      final String servicesNodeName = path + '/' + childName;
      ZoolUtil.debugIf(() -> "Storing service host list for: " + childName);
      millServiceMap.computeIfAbsent(servicesNodeName, p -> new HashSet<>());
      collectNodeChildren(getPubDns(), servicesNodeName);
    });

    // watch for our service node
    this.zoolClient.drain(
        new ZoolDataSinkImpl(this.zoolClient.getServiceMapNode() + '/' + serviceKey, this::onServiceNodeData,
                             this::onServiceNodeNoData));
    Optional.ofNullable(this.serviceMapCallback).ifPresent(Runnable::run);
  }

  private void onZoolServiceMapNoData(String path) {
    LOG.info("onZoolServiceMapNoData: " + path);
    ZoolUtil.createEmptyPersistentNode(zoolClient, path);
  }

  private void onZoolHostNodeData(String path, byte[] data) {
    LOG.info("onZoolHostNodeData: " + path + ", " + data.length + " bytes");
  }

  private void onZoolHostNodeNoData(String path) {
    LOG.info("onZoolHostNodeNoData: " + path);
  }

  private void onServiceNodeData(String serviceNodePath, byte[] data) {
    NetworkUtil.getMaybeCanonicalLocalhostName().ifPresent(localhostUrl -> {
      ZoolUtil.debugIf(() -> "Booting up a " + (isProd ? "Production" : "Dev") + " host@" + localhostUrl);
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
    ZoolUtil.debugIf(() -> "Collect Node Children " + pubDns + " -> " + serviceNodePath);
    List<String> children = zoolClient.getChildren(serviceNodePath);

    // clear the service map
    millServiceMap.computeIfAbsent(serviceNodePath, p -> new HashSet<>());
    millServiceMap.get(serviceNodePath).clear();

    final CountDownLatch latch = new CountDownLatch(children.size());
    children.forEach(childName -> {
      zoolClient.getZookeeper().getData(serviceNodePath + '/' + childName, false, (rc, p, ctx, d, s) -> {
        ZoolUtil.debugIf(() -> "Service Node: " + new String(d));
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
        ZoolUtil.debugIf(() -> "Self Announcing on Node: " + pubDns);
        myHosts.add(pubDns);
      }
    }

    AtomicInteger hostCount = new AtomicInteger(0);
    millServiceMap.forEach((k, v) -> v.forEach(service -> hostCount.incrementAndGet()));

    ZoolUtil.debugIf(() -> "Total External Hosts Known: " + (hostCount.get() - 1));
    // wait for more than just OUR host to be online.
    if(hostCount.get() > 0) {
      if(firstLoad) {
        firstLoad = false;
        Optional.ofNullable(hostsLoadedCallback).ifPresent(Runnable::run);
        // this one happens only once
        hostsLoadedCallback = null;
      } else {
        Optional.ofNullable(hostUpdatedCallback).ifPresent(Runnable::run);
      }
    } else {
      ZoolUtil.debugIf(() -> "Polling for hosts, not yet loaded...");
    }

    if (LOG.isDebugEnabled()) {
      ZoolUtil.debugIf(() -> "Milli Service Nodes are Loaded: " + millServiceMap.size() + " services");
      millServiceMap.forEach((serviceName, hosts) -> {
        ZoolUtil.debugIf(() -> "Service: " + serviceName + ", hosts: ");
        hosts.forEach(hostName -> ZoolUtil.debugIf(() -> "\t - " + hostName));
      });
    }

    // keep polling
    if(millServiceMap.isEmpty() || hostCount.get() <= 1) {
      LOG.info("Polling again since host count only include ourselves");
      executorService.submit(() -> this.poll(pubDns, serviceNodePath));
    }
  }

  /**
   * Called internally by the thread submitted to executor service.
   *
   * @param pubDns          the public dns.
   * @param serviceNodePath the service instance path
   */
  private void poll(String pubDns, String serviceNodePath) {
    ZoolUtil.debugIf(() -> "Poll: " + pubDns + " -> " + serviceNodePath);
    if (zoolClient.isConnected()) {
      try {
        Thread.sleep(pollingInterval);
        this.collectNodeChildren(pubDns, serviceNodePath);
      } catch (InterruptedException ex) {
        zoolClient.disconnect();
        LOG.error("Disconnected from Zookeeper", ex);
        throw new RuntimeException("Disconnected", ex);
      }
    } else {
      LOG.warn("Zool is not connected!");
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
    ZoolUtil.debugIf(() ->"bootServicesHub -> " + serviceNodePath + " on " + localhostUrl);
    String pubDns = getPubDns();

    collectNodeChildren(pubDns, serviceNodePath);
    final String instancePath = serviceNodePath + '/' + localhostUrl + ':' + getCurrentPort();

    // watch for service map node changes
    // when new services come online, or die
    this.zoolClient.drain(new ZoolDataSinkImpl(instancePath, this::onZoolHostNodeData, this::onZoolHostNodeNoData));
    ZoolUtil.createEphemeralNode(zoolClient, instancePath, pubDns.getBytes());

    Optional.ofNullable(this.serviceNodeCallback).ifPresent(Runnable::run);
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
