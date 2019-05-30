package com.decoded.zool;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.decoded.zool.ZoolUtil.debugIf;


/**
 * This class wraps Zool and interacts with it as a Service announcement portal. Each Application Container that uses a
 * ZoolServiceHub can define a service key for every container with the same service key, each container will be grouped
 * into a list of hosts for that service key. Each host on the Zool Cluster will get a copy of the entire services map.
 * Which is how each host knows about all other services, and their locations inherently.
 */
public class ZoolServiceHub {
  // only reset index counts every so often

  private static final String HTTP_PORT_PROP = "http.port";
  private static final Logger LOG = LoggerFactory.getLogger(ZoolServiceHub.class);
  private final Zool zoolClient;
  private final ExecutorService executorService;
  private boolean firstLoad = true;
  private String serviceKey = "milliContainerService";
  private int pollingInterval = 5000;
  private boolean isProd = false;
  private Map<String, Set<String>> millServiceMap = new ConcurrentHashMap<>();
  private int port = -1;

  private Runnable gatewayCallback;
  private Runnable announceCallback;
  private Runnable serviceNodeCallback;
  private Runnable serviceMapCallback;
  private Runnable hostsLoadedCallback;
  private Runnable hostUpdatedCallback;
  private AtomicLong gatewayHostIdx = new AtomicLong(0);
  private AtomicLong hostIdx = new AtomicLong(0);


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

  public ZoolServiceHub setAnnounceCallback(Runnable announceCallback) {
    this.announceCallback = announceCallback;
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
   *
   * @return A set of hosts for a specific service.
   */
  public List<String> getHostsForService(String serviceKey) {
    return new ArrayList<>(millServiceMap.computeIfAbsent(serviceKey, sk -> new HashSet<>()));
  }

  // TODO
  public String getGatewayHost(String serviceKey) {
    // returns the gateway representation of the service key.
    final String gatwayMapNode = this.zoolClient.getGatewayMapNode();
    debugIf(() -> "getGatewayHost[" + serviceKey + "]");

    return getNextHostFromHostList(gatewayHostIdx, getHostsForService(gatwayMapNode + '/' + serviceKey));
  }

  /**
   * Returns the next "best" host to use for a service key.
   *
   * @param serviceKey the service key.
   *
   * @return String the next host.
   */
  public String getNextHost(String serviceKey) {
    // clients should not have to supply the service map node
    final String serviceMap = this.zoolClient.getServiceMapNode();
    final String path = serviceMap + '/' + serviceKey;
    debugIf(() -> "getNextHost[" + path + "]");

    return getNextHostFromHostList(hostIdx, getHostsForService(path));
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

  private String getNextHostFromHostList(AtomicLong hostIdxHolder, List<String> hosts) {
    if (hosts.isEmpty()) {
      LOG.error("Service key " + serviceKey + " was invalid. Valid service keys: ");
      getKnownServices().forEach(knownService -> {
        LOG.warn(knownService);
        getHostsForService(knownService).forEach(host -> LOG.warn("\t" + host));
      });

      return "";
    }

    final long idx = hostIdxHolder.getAndIncrement();

    // corrected idx will always be within range
    final int correctedIdx = (int) (idx % hosts.size());
    debugIf(() -> "Getting " + serviceKey + " host at " + correctedIdx + " of " + hosts.size());

    return hosts.get(correctedIdx);
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
    ZoolUtil.debugIf(
        () -> "onZookeeperServiceMapData: " + path + ", " + data.length + " bytes, value: \n" + new String(data));

    List<String> children = zoolClient.getChildren(path);

    ZoolUtil.debugIf(() -> "Total Known Services: " + children.size());
    children.forEach(childName -> {
      final String servicesNodeName = path + '/' + childName;
      ZoolUtil.debugIf(() -> "Storing service host list for: " + childName);
      millServiceMap.computeIfAbsent(servicesNodeName, p -> new HashSet<>());
      final String pubDns = getPubDns();
      int hostCount = collectServiceNodeChildren(pubDns, servicesNodeName);

      // keep polling
      if (millServiceMap.isEmpty() || hostCount <= 1) {
        LOG.info("Polling again since host count only include ourselves");
        executorService.submit(() -> this.poll(pubDns, servicesNodeName, 750));
      } else {
        executorService.submit(() -> this.poll(pubDns, servicesNodeName, pollingInterval));
      }

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

    ZoolUtil.debugIf(() -> "Booting up a " + (isProd ? "Production" : "Dev"));
    this.bootServicesHub(serviceNodePath);

  }

  /**
   * Collects the children of a specific service node to store the dns of available hosts.
   *
   * @param pubDns          the public dns of this server
   * @param serviceNodePath the service node path
   */
  private int collectServiceNodeChildren(String pubDns, String serviceNodePath) {
    LOG.warn("Collect Service Node Children " + pubDns + " -> " + serviceNodePath);
    List<String> children = zoolClient.getChildren(serviceNodePath);
    Set<String> newHosts = new HashSet<>();

    // load children nodes
    final CountDownLatch latch = new CountDownLatch(children.size());
    ZooKeeper zk = zoolClient.getZookeeper();
    children.forEach(childName -> zk.getData(serviceNodePath + '/' + childName, false, (rc, p, ctx, d, s) -> {
      LOG.info("Received Service Node Announcement: " + new String(d));
      newHosts.add(new String(d));
      latch.countDown();
    }, null));

    try {
      latch.await(zoolClient.getTimeout(), TimeUnit.MILLISECONDS);
      // refresh the list with zero list downtime.

      boolean didAnnounce = false;
      if (serviceNodePath.endsWith(serviceKey)) {
        if (!newHosts.contains(pubDns)) {
          LOG.info("Announcing from Node: " + pubDns);
          newHosts.add(pubDns);
          didAnnounce = true;
        }
      }

      millServiceMap.put(serviceNodePath, newHosts);

      if (didAnnounce) {
        Optional.ofNullable(announceCallback).ifPresent(Runnable::run);
      }

    } catch (InterruptedException ex) {
      throw new RuntimeException("Timed out connecting to Zookeeper", ex);
    }

    AtomicInteger hostCount = new AtomicInteger(0);
    millServiceMap.forEach((k, v) -> v.forEach(service -> hostCount.incrementAndGet()));

    ZoolUtil.debugIf(() -> "Total External Hosts Known: " + (hostCount.get() - 1));

    // wait for more than just OUR host to be online.
    if (hostCount.get() > 0) {
      LOG.info("Host count load: " + hostCount);
      if (firstLoad) {
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

    return hostCount.get();
  }

  /**
   * Called internally by the thread submitted to executor service.
   *
   * @param pubDns          the public dns.
   * @param serviceNodePath the service instance path
   */
  private void poll(String pubDns, String serviceNodePath, long restInterval) {
    LOG.info("Poll: " + pubDns + " -> " + serviceNodePath);
    if (zoolClient.isConnected()) {
      try {
        this.collectServiceNodeChildren(pubDns, serviceNodePath);
        // force a wait
        Thread.sleep(restInterval);
      } catch (InterruptedException ex) {
        zoolClient.disconnect();
        LOG.error("Disconnected from Zookeeper", ex);
        throw new RuntimeException("Disconnected", ex);
      }
    } else {
      LOG.warn("Zool is not connected!");
    }
  }

  /**
   * returns the current http port that the http server is serving on.
   *
   * @return the current port, an integer.
   */
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

  /**
   * Returns the DNS based identifier for this machine.
   *
   * @return a String that uniquely identifies this host in the service key cluster on zookeeper.
   */
  private String getPubDns() {
    // You must set the environment variable denoted in EnvironmentConstants
    // for both dev and prod PUB DNS to run the container application.
    return Optional.ofNullable(
        System.getenv(isProd ? EnvironmentConstants.PROD_SERVER_DNS : EnvironmentConstants.DEV_SERVER_DNS))
        .map(value -> {
          final String pubDns = value + ':' + getCurrentPort();
          LOG.info("Announcing as " + serviceKey + " host on: " + pubDns);
          return pubDns;
        })
        .orElseGet(() -> {
          LOG.error(
              "Environment Expected to have " + EnvironmentConstants.PROD_SERVER_DNS + ", and " + EnvironmentConstants.DEV_SERVER_DNS + " keys");
          System.getenv().forEach((k, v) -> LOG.error(k + " = " + v));
          return null;
        });
  }


  /**
   * Boots the hub with a service node path, and localhost url.
   *
   * @param serviceNodePath service node path
   */
  private void bootServicesHub(final String serviceNodePath) {
    String pubDns = getPubDns();

    if (pubDns == null) {
      throw new IllegalArgumentException("Environment variables are not set for PROD or LOCAL server dns!");
    }
    LOG.info("Booting Service Hub, my node path is: " + serviceNodePath + ", on host: " + pubDns);

    ZoolUtil.debugIf(() -> "bootServicesHub -> " + serviceNodePath + " on " + pubDns + " with pub dns: " + pubDns);
    collectServiceNodeChildren(pubDns, serviceNodePath);
    final String instanceKey = Base64.getEncoder().encodeToString((pubDns + ':' + getCurrentPort()).getBytes());
    final String instancePath = serviceNodePath + '/' + instanceKey;

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
