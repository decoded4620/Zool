package com.decoded.zool;

import com.decoded.javautil.Pair;
import com.decoded.zool.connection.ZookeeperConnection;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


/**
 * An abstract Zool Service Mesh Client which can be extended for querying a Zool Service. You must implement the
 * Discovery Service, and then extend this abstract and implement the methods to remotely announce your service. This
 * allows you to specify the transport mechanism, and normalized formats.
 */
public abstract class ZoolServiceMeshClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolServiceMeshClient.class);
  private final ZoolReader zoolReader;
  private Map<String, Map<String, ZoolAnnouncement>> zoolServiceMesh = new HashMap<>();
  private Map<String, AtomicLong> hostIdxMap = new HashMap<>();
  private Map<String, ZoolAnnouncement> gatewayHosts = new HashMap<>();
  private AtomicLong gatewayHostIdx = new AtomicLong(0);
  private String zoolServiceKey = "";
  private String zoolGatewayKey = "";
  private boolean announced = false;
  private boolean gatewayConnected = false;
  private ZoolConfig zoolConfig;

  // map of host indexes (for predictable load balancing)
  private boolean isProd;

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final ZookeeperConnection zookeeperConnection;

  /**
   * Constructor
   *
   * @param zoolConfig a zool configuration
   * @param zookeeperConnection a zookeeper connection
   * @param zoolReader a Zool reader.
   */
  public ZoolServiceMeshClient(ZoolConfig zoolConfig, ZookeeperConnection zookeeperConnection, ZoolReader zoolReader) {
    this.zoolConfig = zoolConfig;
    this.zoolReader = zoolReader;
    this.zookeeperConnection = zookeeperConnection;
  }

  public ZoolConfig getZoolConfig() {
    return zoolConfig;
  }

  public ZookeeperConnection getZookeeperConnection() {
    return zookeeperConnection;
  }

  /**
   * Returns <code>true</code> if this client is announced on the network, and has knowledge of the other hosts.
   *
   * @return a boolean.
   */
  public boolean isAnnounced() {
    return announced;
  }

  /**
   * Set the announcement flag.
   *
   * @param announced true if we are announced
   *
   * @return this client
   */
  protected ZoolServiceMeshClient setAnnounced(boolean announced) {
    this.announced = announced;
    return this;
  }

  /**
   * returns true if the gateway is connected (e.g. we know about gateway hosts for announcement)
   *
   * @return a boolean
   */
  public boolean isGatewayConnected() {
    return gatewayConnected;
  }

  /**
   * Returns <code>true</code> if this is a prod client.
   *
   * @return boolean
   */
  public boolean isProd() {
    return isProd;
  }

  /**
   * Set the production (vs development) flag.
   *
   * @param prod the prod flag, false for dev
   *
   * @return the client.
   */
  public ZoolServiceMeshClient setProd(final boolean prod) {
    isProd = prod;
    return this;
  }

  /**
   * Update the service mesh directly. If an announce method is passed in it is invoked in the event that the current
   * host is not known in the updated map. (this re-announces)
   *
   * @param freshMesh the new data to update with.
   * @param announcer the announcer function that returns a future of some type
   * @param <X>       type returned by the announcer future
   *
   * @return a {@link CompletableFuture} of X
   */
  public <X> CompletableFuture<X> updateServiceMesh(Map<String, Map<String, ZoolAnnouncement>> freshMesh,
      Function<Boolean, CompletableFuture<X>> announcer) {
    LOG.info("updating service mesh: " + freshMesh.size() + " service keys");
    // default update
    updateServiceMesh(freshMesh);

    // check for discoverability
    if (!isMyHostDiscoverable()) {
      // announce again
      LOG.warn("Re-Announcing ourselves to zool!");
      return announcer.apply(isProd());
    }

    return CompletableFuture.completedFuture(null);
  }

  /**
   * Update the service mesh without option to reannounce
   *
   * @param freshMesh the new mesh
   */
  public void updateServiceMesh(Map<String, Map<String, ZoolAnnouncement>> freshMesh) {
    LOG.info("updating service mesh: " + freshMesh);
    clearZoolServiceMesh();
    freshMesh.forEach(zoolServiceMesh::put);
  }

  /**
   * Returns <code>true</code> if the current zool service mesh contains our host
   *
   * @return a boolean
   */
  protected boolean isMyHostDiscoverable() {
    boolean isDiscoverable;
    if (!zoolServiceMesh.containsKey(getZoolServiceKey())) {
      LOG.info("Service key: " + getZoolServiceKey() + " is not known to central discovery service yet...");
      isDiscoverable = false;
    } else {
      final Map<String, ZoolAnnouncement> hostsForMyService = zoolServiceMesh.get(getZoolServiceKey());
      // return true if our host is known on the service mesh.
      isDiscoverable = hostsForMyService != null && hostsForMyService.keySet()
          .stream()
          .anyMatch(key -> key.equals(
              ZoolSystemUtil.getLocalHostUrlAndPort(isProd(), ZoolSystemUtil.isSecure(), zoolConfig)));

      LOG.info(
          "My service exists at: " + getZoolServiceKey() + ", is my host visible to Discovery Services? " + isDiscoverable);
    }
    return isDiscoverable;
  }

  /**
   * Ignore a single host from a client perspective.
   *
   * @param serviceKey the service key
   * @param hostUrl    the host url to ignore
   */
  public void ignoreHost(String serviceKey, String hostUrl) {
    Map<String, ZoolAnnouncement> set = zoolServiceMesh.get(serviceKey);
    if (set != null) {
      set.keySet().stream().filter(hostUrl::equals).findFirst().ifPresent(set::remove);
      LOG.info("Ignoring host: " + hostUrl);
    } else {
      LOG.info("Host " + hostUrl + " is unknown or already ignored");
    }
  }

  /**
   * Clear the service mesh (internal use, and for implementations of the zool client).
   */
  protected void clearZoolServiceMesh() {
    LOG.info("Clearing service mesh...");
    zoolServiceMesh = new ConcurrentHashMap<>();
  }

  /**
   * Returns a copy of the service mesh. This is a modifiable map, but is a copy of the current mesh network for
   * safety.
   *
   * @return the current mesh network.
   */
  public Map<String, Map<String, ZoolAnnouncement>> getZoolServiceMesh() {
    HashMap<String, Map<String, ZoolAnnouncement>> meshCopy = new HashMap<>();
    zoolServiceMesh.forEach((key, hosts) -> meshCopy.put(key, new HashMap<>(hosts)));
    return meshCopy;
  }

  /**
   * Get (or create) a service fabric locally for the service key in our local mesh.
   *
   * @param serviceKey the service key
   *
   * @return a set of String values for hosts on the service.
   */
  public Map<String, ZoolAnnouncement> getOrCreateServiceFabric(String serviceKey) {
    return zoolServiceMesh.computeIfAbsent(serviceKey, sn -> new HashMap<>());
  }

  /**
   * Returns this clients service key.
   *
   * @return a string.
   */
  public String getZoolServiceKey() {
    return zoolServiceKey;
  }

  /**
   * Change the service key for this client. It will announce on this service key when running.
   *
   * @param zoolServiceKey the service key to use.
   *
   * @return this client.
   */
  public ZoolServiceMeshClient setZoolServiceKey(final String zoolServiceKey) {
    this.zoolServiceKey = zoolServiceKey;
    return this;
  }

  /**
   * the Zool gateway key (which contains gateway hosts). These hosts are made available to zool clients to use for
   * network calls into the mesh.
   *
   * @return a String
   */
  public String getZoolGatewayKey() {
    return zoolGatewayKey;
  }

  /**
   * Change the zool gateway key to point to a custom dynamic discovery service.
   *
   * @param zoolGatewayKey the custom gateway key.
   *
   * @return this client
   */
  public ZoolServiceMeshClient setZoolGatewayKey(final String zoolGatewayKey) {
    this.zoolGatewayKey = zoolGatewayKey;
    return this;
  }

  /**
   * Gets the next gateway host url.
   *
   * @return the next host url
   */
  public Pair<String, ZoolAnnouncement> getGatewayHostUrl() {
    return ZoolServiceMesh.getNextHostFromHostAnnouncementList(gatewayHostIdx, gatewayHosts);
  }

  /**
   * Gets the set of known gateway hosts
   *
   * @return a set of strings
   */
  public Map<String, ZoolAnnouncement> getGatewayHosts() {
    return gatewayHosts;
  }

  /**
   * Connect to zookeeper server
   *
   * @return this client.
   */
  public ZoolServiceMeshClient connect() {
    zoolReader.getZool().connect();
    return this;
  }

  /**
   * Initializes the client gateway knowledge. This will connect directly to zookeeper and get the gateway hosts.
   *
   * @return the chosen gateway host.
   */
  public CompletableFuture<Map<String, ZoolAnnouncement>> getDiscoveryGatewayHosts() {
    if (zoolServiceKey == null || zoolServiceKey.isEmpty() || zoolGatewayKey == null || zoolGatewayKey.isEmpty()) {
      throw new IllegalStateException(
          "You must specify a service key and gateway key for the zool service mesh client");
    }

    gatewayConnected = false;
    // join the path with zk separator
    final String gatewayPath = ZConst.PathSeparator.ZK.join(zoolReader.getZool().getServiceMapNode(), zoolGatewayKey);
    // we will load the gateway service path once, and then talk to one of the hosts which handle gateway
    // discovery to get updates. This avoids overloading zookeeper nodes.
    LOG.info("Zool Service Client is connecting to zool gateway: " + zoolGatewayKey + " path: " + gatewayPath);
    CompletableFuture<Map<String, ZoolAnnouncement>> serviceMeshFuture = new CompletableFuture<>();
    final Map<String, ZoolAnnouncement> newGatewayHosts = new HashMap<>();

    zoolReader.readChildren(gatewayPath, (p, hosts) -> {
      LOG.info("Loaded " + hosts.size() + " host pairs at: " + gatewayPath);
      executorService.submit(() -> {
        CountDownLatch latch = new CountDownLatch(hosts.size());

        hosts.forEach(host -> {
          final String servicePath = ZConst.PathSeparator.ZK.join(gatewayPath, host);
          LOG.info("Reading child data from: " + servicePath);
          zoolReader.readChannel(servicePath, (channelKey, data) -> {
            LOG.info("Data for Zool Channel: " + channelKey + " data: " + data.length);
            ZoolAnnouncement announcement = ZoolAnnouncement.deserialize(data);
            newGatewayHosts.put(host, announcement);
            latch.countDown();
          }, hostNoData -> {
            LOG.warn("Host: " + hostNoData + " had no data");
            latch.countDown();
          });
        });

        boolean awaitSuccess;
        try {
          if (!latch.await(zookeeperConnection.getZkConnectTimeout() * hosts.size(), TimeUnit.MILLISECONDS)) {
            LOG.warn(
                "Timeout while waiting... loaded " + newGatewayHosts.size() + " of " + hosts.size() + " hosts");
          } else {
            LOG.info ("Loaded all hosts: " + newGatewayHosts.size());
          }
        } catch (InterruptedException ex) {
          LOG.warn(
              "Interrupted while waiting... only loaded " + newGatewayHosts.size() + " of " + hosts.size() + " hosts");
        }

        gatewayHosts = ImmutableMap.copyOf(newGatewayHosts);

        // wait for at least one host.
        if (!gatewayHosts.isEmpty()) {
          LOG.info("Gateway Hosts received for " + p + " , " + hosts.size());
          gatewayConnected = true;
          serviceMeshFuture.complete(gatewayHosts);
        } else {
          LOG.warn("No Gateway Hosts found announced on zookeeper node: " + p + " waiting for connection...");
          // the future should wait
        }
      });
      //      sinks.forEach(zoolReader.getZool()::drainStop);
    }, p -> {
      LOG.warn("No Gateway Service announced on zookeeper: " + p + " waiting for announcement...");
      // the future should wait
    });

    return serviceMeshFuture;
  }

  /**
   * Disconnect from zookeeper server
   */
  public void disconnect() {
    LOG.warn("Stopping Zool Client!");
    zoolReader.getZool().disconnect();
  }

  /**
   * Returns the set of hosts for a service.
   *
   * @param serviceKey the zookeeper service key
   *
   * @return A set of hosts for a specific service.
   */
  public Map<String, ZoolAnnouncement> getServiceHosts(String serviceKey) {
    Map<String, ZoolAnnouncement> hosts = zoolServiceMesh.get(serviceKey);
    if (hosts == null) {
      return Collections.emptyMap();
    }
    return hosts;
  }

  /**
   * Gets a set of known services from zookeeper.
   *
   * @return a Set of string service names.
   */
  public Set<String> getServices() {
    return zoolServiceMesh.keySet();
  }

  /**
   * Returns the next "best" host to use for a service key.
   *
   * @param serviceKey the service key.
   *
   * @return String the next host.
   */
  public Pair<String, ZoolAnnouncement> getNextServiceHost(String serviceKey) {
    // clients should not have to supply the service map node
    return ZoolServiceMesh.getNextHostFromHostAnnouncementList(
        hostIdxMap.computeIfAbsent(serviceKey, sk -> new AtomicLong(0)), getServiceHosts(serviceKey));
  }
}
