package com.decoded.zool;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * An abstract Zool Service Mesh Client which can be extended for querying a Zool Service. You must implement the
 * Discovery Service, and then extend this abstract and implement the methods to remotely announce your service. This
 * allows you to specify the transport mechanism, and normalized formats.
 */
public abstract class ZoolServiceMeshClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolServiceMeshClient.class);
  private final ZoolReader zoolReader;
  private Map<String, Set<String>> zoolServiceMesh = new HashMap<>();
  private Map<String, AtomicLong> hostIdxMap = new HashMap<>();
  private List<String> gatewayHosts = new ArrayList<>();
  private AtomicLong gatewayHostIdx = new AtomicLong(0);
  private String zoolServiceKey = "";
  private String zoolGatewayKey = "";
  private boolean isAnnounced = false;
  // map of host indexes (for predictable load balancing)
  private boolean isProd;

  /**
   * Constructor
   * @param zoolReader a Zool reader.
   */
  public ZoolServiceMeshClient(ZoolReader zoolReader) {
    this.zoolReader = zoolReader;
  }

  /**
   * Returns <code>true</code> if this client is announced on the network, and has knowledge of the other hosts.
   * @return a boolean.
   */
  public boolean isAnnounced() {
    return isAnnounced;
  }

  /**
   * Returns <code>true</code> if this is a prod client.
   * @return boolean
   */
  public boolean isProd() {
    return isProd;
  }

  /**
   * Set the production (vs development) flag.
   * @param prod the prod flag, false for dev
   * @return the client.
   */
  public ZoolServiceMeshClient setProd(final boolean prod) {
    isProd = prod;
    return this;
  }

  /**
   * update service mesh directly.
   *
   * @param freshMesh the new data to update with.
   */
  public void updateServiceMesh(Map<String, Set<String>> freshMesh) {
    LOG.info("update service mesh: " + freshMesh);
    zoolServiceMesh.clear();
    freshMesh.forEach(zoolServiceMesh::put);
  }

  /**
   * Clear the service mesh (internal use, and for implementations of the zool client).
   */
  protected void clearZoolServiceMesh() {
    zoolServiceMesh = new ConcurrentHashMap<>();
  }

  /**
   * Returns a copy of the service mesh. This is a modifiable map, but is a copy of the current mesh network for
   * safety.
   *
   * @return the current mesh network.
   */
  public Map<String, Set<String>> getZoolServiceMesh() {
    HashMap<String, Set<String>> meshCopy = new HashMap<>();
    zoolServiceMesh.forEach((key, hosts) -> meshCopy.put(key, new HashSet<>(hosts)));
    return meshCopy;
  }

  /**
   * Get (or create) a service fabric locally for the service key in our local mesh.
   * @param serviceKey the service key
   * @return a set of String values for hosts on the service.
   */
  public Set<String> getOrCreateServiceFabric(String serviceKey) {
    return zoolServiceMesh.computeIfAbsent(serviceKey, sn -> new HashSet<>());
  }

  /**
   * Returns this clients service key.
   * @return a string.
   */
  public String getZoolServiceKey() {
    return zoolServiceKey;
  }

  /**
   * Change the service key for this client. It will announce on this service key when running.
   * @param zoolServiceKey the service key to use.
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
   * @param zoolGatewayKey the custom gateway key.
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
  public String getGatewayHostUrl() {
    return ZoolServiceMesh.getNextHostFromHostList(gatewayHostIdx, gatewayHosts);
  }

  /**
   * Gets the set of known gateway hosts
   *
   * @return a set of strings
   */
  public List<String> getGatewayHosts() {
    return gatewayHosts;
  }

  /**
   * Initializes the client gateway knowledge. This will connect directly to zookeeper and get the gateway hosts.
   *
   * @return the chosen gateway host.
   */
  public CompletableFuture<List<String>> start() {
    if (zoolServiceKey == null || zoolServiceKey.isEmpty() || zoolGatewayKey == null || zoolGatewayKey.isEmpty()) {
      throw new IllegalStateException(
          "You must specify a service key and gateway key for the zool service mesh client");
    }

    LOG.info("Service Gateway Client starting, gateway key is : " + zoolGatewayKey);

    CompletableFuture<List<String>> serviceMeshFuture = new CompletableFuture<>();
    final String gatewayPath = zoolReader.getZool().getServiceMapNode() + '/' + zoolGatewayKey;
    // we will load the gateway service path once, and then talk to one of the hosts which handle gateway
    // discovery to get updates. This avoids overloading zookeeper nodes.

    LOG.info("Connecting to Zool Node " + gatewayPath);
    zoolReader.getZool().connect();
    zoolReader.readChildren(gatewayPath, (p, hosts) -> {
      LOG.info("Gateway Hosts received for " + p + " , " + hosts.size());
      gatewayHosts = ImmutableList.copyOf(hosts);
      isAnnounced = true;
      serviceMeshFuture.complete(gatewayHosts);
    }, p -> {
      LOG.warn("No Gateway Hosts announced at gateway: " + p);
      serviceMeshFuture.complete(gatewayHosts);
    }).oneOff();

    serviceMeshFuture.thenRun(zoolReader.getZool()::disconnect);

    return serviceMeshFuture;
  }

  /**
   * Stop the client connection completely, resetting the announcement flag.
   */
  public void stop() {
    zoolReader.getZool().disconnect();
    isAnnounced = false;
  }

  /**
   * Returns the set of hosts for a service.
   *
   * @param serviceKey the zookeeper service key
   *
   * @return A set of hosts for a specific service.
   */
  public List<String> getServiceHosts(String serviceKey) {
    Set<String> hosts = zoolServiceMesh.get(serviceKey);
    if (hosts == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(hosts);
  }

  /**
   * Gets a set of known services from zookeeper.
   *
   * @return a Set of string service names.
   */
  public List<String> getServices() {
    return new ArrayList<>(zoolServiceMesh.keySet());
  }

  /**
   * Returns the next "best" host to use for a service key.
   *
   * @param serviceKey the service key.
   *
   * @return String the next host.
   */
  public String getNextServiceHost(String serviceKey) {
    // clients should not have to supply the service map node
    return ZoolServiceMesh.getNextHostFromHostList(hostIdxMap.computeIfAbsent(serviceKey, sk -> new AtomicLong(0)),
        getServiceHosts(serviceKey));
  }
}
