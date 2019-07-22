package com.decoded.zool;

import com.decoded.stereohttp.RequestMethod;
import com.decoded.stereohttp.RestRequest;
import com.decoded.stereohttp.StereoHttpClient;
import com.decoded.stereohttp.StereoHttpTask;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.decoded.zool.ZoolLoggingUtil.debugIf;
import static com.decoded.zool.ZoolLoggingUtil.infoIf;
import static com.decoded.zool.ZoolSystemUtil.getLocalHostUrl;
import static com.decoded.zool.ZoolSystemUtil.getLocalHostUrlAndPort;


/**
 * This class wraps Zool and interacts with it as a Service announcement portal. Each Application Container that uses a
 * ZoolServiceMesh can define a service key for every container with the same service. Each container will be grouped
 * into a list of hosts for that service key. Each host on the Zool Cluster will get a copy of the entire services map.
 * This is how each host knows about all other services, and their locations inherently.
 * <p>
 * Zool Service Mesh will periodically run cleaning services (e.g. when new hosts / services are announced or removed)
 * which pings the health check endpoint of all known hosts. Any non-responsive hosts are removed from the pool.
 */
public class ZoolServiceMesh {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolServiceMesh.class);
  // 5 minutes in milliseconds
  private static final int TOKEN_EXPIRY_TIME = 1000 * 60 * 5;
  private static final long MIN_TIME = 15000L;

  // these intervals start aggressive (small times) and get larger as they "ramp" up. each loop of the intervals
  // will grow these intervals until they "mature" to their max value.
  private static final ElasticInterval internalCleanInterval = ElasticInterval.elasticRamp(.15, MIN_TIME,
      TOKEN_EXPIRY_TIME / 5);
  private static final ElasticInterval serviceCleanScheduleInterval = ElasticInterval.elasticRamp(.10, MIN_TIME, 3000);
  private final StereoHttpClient stereoHttpClient;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Map<String, Set<String>> meshNetwork = new ConcurrentHashMap<>();
  private final Map<String, Set<String>> missingReports = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Runnable>> scheduledMissingHostChecks = new ConcurrentHashMap<>();
  private int serviceHealthCheckTimeout = 500;
  private String discoveryHealthCheckEndpoint = "/discoveryHealthCheck";
  private boolean healthCheckRunning = false;
  private boolean healthCheckScheduled = false;
  private boolean isAnnounced = false;
  private String zoolServiceKey = "";
  private ZoolAnnouncement zoolGatewayAnnouncement = new ZoolAnnouncement();
  private boolean isProd = false;
  private int port = -1;
  private ZoolWriter zoolWriter;
  private ZoolReader zoolReader;
  private boolean announcementChangeScheduled = false;
  private MeshState meshState = MeshState.STOPPED;

  @Inject
  public ZoolServiceMesh(ZoolReader zoolReader,
      ZoolWriter zoolWriter,
      ExecutorService executorService,
      ScheduledExecutorService scheduledExecutorService,
      StereoHttpClient stereoHttpClient) {
    this.zoolReader = zoolReader;
    this.zoolWriter = zoolWriter;
    this.executorService = executorService;
    this.scheduledExecutorService = scheduledExecutorService;
    this.stereoHttpClient = stereoHttpClient;
  }

  /**
   * Generates an instance token for a host uri, and base64 encodes it. A new token value is generated each minute.
   *
   * @param hostUri the host uri, e.g. <code>"localhost:9000"</code>
   *
   * @return a byte[] for the new token generated.
   */
  public static byte[] getInstanceToken(String hostUri) {
    // generates a new token every 5 minutes
    return Base64.getEncoder().encode((hostUri + "::" + System.currentTimeMillis() / TOKEN_EXPIRY_TIME).getBytes());
  }

  /**
   * This method returns the next host in a list of hosts, based ont he atomic index for that sent of hosts. in this
   * case it is naive, and the index is incremented and modulus operator is applied.
   *
   * @param hostIdxHolder the atomic index
   * @param hosts         the set of hosts
   *
   * @return the chosen host or empty
   */
  public static String getNextHostFromHostList(AtomicLong hostIdxHolder, List<String> hosts) {
    if (hosts.isEmpty()) {
      LOG.warn("no hosts were supplied to getNextHostsFromHostList!");
      return "";
    }

    // corrected idx will always be within range
    return hosts.get((int) (hostIdxHolder.getAndIncrement() % hosts.size()));
  }

  /**
   * Returns the last known mesh state based on connectivity to zookeeper. Only used as a status, no logic within this
   * class depends on the value of this state.
   *
   * @return the {@link MeshState}
   */
  public MeshState getMeshState() {
    return meshState;
  }

  /**
   * The timeout value for service healthcheck.
   *
   * @param serviceHealthCheckTimeout the timeout value.
   *
   * @return ZoolServiceMesh
   */
  public ZoolServiceMesh setServiceHealthCheckTimeout(final int serviceHealthCheckTimeout) {
    this.serviceHealthCheckTimeout = serviceHealthCheckTimeout;
    return this;
  }

  /**
   * The endpoint that we'll use for discovery healthcheck. This is the URI that Zool will use to ping hosts that have
   * announced on a cluster.
   *
   * @return a string, the endpoint uri, e.g. /healthCheck
   */
  public String getDiscoveryHealthCheckEndpoint() {
    return discoveryHealthCheckEndpoint;
  }

  /**
   * Set a custom endpoint for health checks.
   *
   * @param discoveryHealthCheckEndpoint some uri (not including scheme, host, or port) for a healthcheck against your
   *                                     hosts.
   *
   * @return this {@link ZoolServiceMesh}
   */
  public ZoolServiceMesh setDiscoveryHealthCheckEndpoint(final String discoveryHealthCheckEndpoint) {
    this.discoveryHealthCheckEndpoint = discoveryHealthCheckEndpoint;
    return this;
  }

  /**
   * Returns <code>true</code> if we've announced on the network
   *
   * @return boolean
   */
  public boolean isAnnounced() {
    return isAnnounced;
  }

  /**
   * Returns the service key for this service mesh
   *
   * @return the current service key.
   */
  public String getZoolServiceKey() {
    return zoolServiceKey;
  }

  /**
   * Change the zool service key.
   *
   * @param zoolServiceKey the new key
   *
   * @return this service mesh.
   */
  public ZoolServiceMesh setZoolServiceKey(final String zoolServiceKey) {
    this.zoolServiceKey = zoolServiceKey;
    return this;
  }

  /**
   * production flag.
   *
   * @return true if this is a production discovery service.
   */
  public boolean isProd() {
    return isProd;
  }

  /**
   * Set the production flag.
   *
   * @param prod a boolean, true if this is a prod host.
   *
   * @return this service mesh.
   */
  public ZoolServiceMesh setProd(final boolean prod) {
    isProd = prod;
    return this;
  }

  /**
   * Returns the current port for this service host.
   *
   * @return an integer.
   */
  public int getPort() {
    return port;
  }

  /**
   * Set the host port
   *
   * @param port an integer
   *
   * @return this mesh.
   */
  public ZoolServiceMesh setPort(final int port) {
    this.port = port;
    return this;
  }

  /**
   * Public api to attach custom zool channel readers
   *
   * @return a {@link ZoolReader}
   */
  public ZoolReader getZoolReader() {
    return zoolReader;
  }

  /**
   * Public api to attach custom zool channel writers
   *
   * @return a {@link ZoolWriter}
   */
  public ZoolWriter getZoolWriter() {
    return zoolWriter;
  }

  /**
   * Start the service mesh.
   *
   * @return a completable future.
   */
  public CompletableFuture<Map<String, Set<String>>> start() {
    if (zoolServiceKey == null || zoolServiceKey.isEmpty()) {
      throw new IllegalStateException(
          "You must supply a zool service key for the service mesh. Here are some examples of good service mesh " +
              "keys:" + " " + "'discovery', 'gateway', or 'servicediscovery'.");
    }

    meshState = MeshState.STARTING;

    CompletableFuture<Map<String, Set<String>>> serviceMeshFuture = new CompletableFuture<>();

    // the service map node
    zoolReader.readChannelAndChildren(zoolReader.getZool().getServiceMapNode(), (servicePath, bytes) -> {
      LOG.info("Service Map is available, waiting on children...");
    }, serviceMapPath -> {
      LOG.warn("Service Map Node is not available, creating: " + serviceMapPath);
      if (!zoolWriter.createPersistentNode(serviceMapPath)) {
        LOG.error("Node not created: " + serviceMapPath);
      }
    }, (serviceMapPath, serviceNames) -> {
      LOG.info("Service: " + serviceMapPath + " received " + serviceNames.size() + " services");
      // if we were stopped prior to this point, bail
      // attempt to announce if children are found, but we're not in the list.
      this.announceSelfAsGateway(zoolServiceKey);

      // update again
      updateHostsForAllServicesFromZool(serviceNames).thenAccept(voidT -> serviceMeshFuture.complete(getMeshNetwork()));
      meshState = MeshState.RUNNING;

    }, serviceMapPath -> {
      // if no children are found, we'll announce ourselves now
      this.announceSelfAsGateway(zoolServiceKey);
      meshState = MeshState.RUNNING;
      serviceMeshFuture.complete(getMeshNetwork());
    });

    infoIf(LOG, () -> "Connecting to Zool Service..");

    zoolReader.getZool().connect();
    return serviceMeshFuture;
  }

  /**
   * Stop the service mesh. This will destroy all ephemeral nodes (created from this connection).
   */
  public void stop() {
    debugIf(LOG, () -> "Stopping Service Mesh");
    meshState = MeshState.STOPPING;
    zoolReader.getZool().disconnect();
    meshState = MeshState.STOPPED;
    debugIf(LOG, () -> "Stopped Service Mesh");
  }

  /**
   * Returns the map of services
   *
   * @return the service mesh map
   */
  public Map<String, Set<String>> getMeshNetwork() {
    return ImmutableMap.copyOf(meshNetwork);
  }

  /**
   * Reads the data for child nodes of each known service
   *
   * @param serviceKeys the known services
   *
   * @return a Completable future
   */
  private CompletableFuture<Void> updateHostsForAllServicesFromZool(List<String> serviceKeys) {
    infoIf(LOG, () -> "Updating hosts from Zool for " + serviceKeys.size() + " services");
    if (!serviceKeys.isEmpty()) {
      return CompletableFuture.supplyAsync(() -> {

        Set<String> existingServices = meshNetwork.keySet();
        // remove any keys that are no longer known to zookeeper
        existingServices.stream().filter(key -> !serviceKeys.contains(key)).forEach(meshNetwork::remove);

        serviceKeys.forEach(serviceKey -> {
          // inner jobs?
          final String servicePath = ZConst.PathSeparator.ZK.join(getZoolReader().getZool().getServiceMapNode(),
              serviceKey);

          if (!zoolReader.isReading(servicePath)) {
            zoolReader.readChildren(servicePath, (p, c) -> {
              // only care about the service hosts loading here here.
              meshNetwork.remove(serviceKey);
              meshNetwork.computeIfAbsent(serviceKey, x -> new HashSet<>()).addAll(c);

              infoIf(LOG, () -> "Updated mesh for " + serviceKey + " to " + c.size() + " total hosts");
            }, p -> LOG.warn("No hosts found on path " + p));
          } else {
            LOG.warn("Not reading children of " + servicePath + ", its already being read");
          }
        });

        int totalHosts = meshNetwork.values().stream().mapToInt(Set::size).sum();
        infoIf(LOG,
            () -> "Loaded Total Services: " + meshNetwork.size() + " services and " + totalHosts + " total hosts");
        return null;
      }, executorService);
      // update the hosts for each service in a single thread
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Announces this host as a gateway host to Zookeeper
   *
   * @param zoolGatewayKey gateway service node key
   */
  protected void announceSelfAsGateway(final String zoolGatewayKey) {
    if (!this.isAnnounced) {
      infoIf(LOG, () -> "Self Announcing Service mesh host on service key: " + zoolGatewayKey);
      zoolGatewayAnnouncement = announceServiceHost(zoolGatewayKey, getLocalHostUrlAndPort(isProd, port));

      if (zoolGatewayAnnouncement == null || zoolGatewayAnnouncement.token.length == 0) {
        LOG.error("Announcement failure", new ZoolServiceException("Announcement was a failure"));
      } else {
        isAnnounced = true;

        if (stereoHttpClient.canStart()) {
          // start stereo
          stereoHttpClient.start();
        }

        // initialize the network health check
        scheduleNetworkHealthCheck();
      }
    } else {
      infoIf(LOG, () -> "this is not the first update...");
    }
  }

  /**
   * Schedules a network health check for all known hosts.
   */
  private void scheduleNetworkHealthCheck() {
    infoIf(LOG, () -> "Scheduling Network health check in: " + internalCleanInterval + " ms");
    if (!healthCheckRunning && !healthCheckScheduled) {
      healthCheckScheduled = true;
      scheduledExecutorService.schedule(() -> {
        healthCheckScheduled = false;
        this.networkHealthCheck().thenRun(() -> executorService.submit(this::scheduleNetworkHealthCheck));
      }, internalCleanInterval.getAndUpdate(), TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Schedules a single health check on a host address
   *
   * @param hostAddress a HostAddress object to health check
   */
  private void scheduleHostHealthCheck(final HostAddress hostAddress) {
    LOG.info("Scheduling an ad-hoc health check for host node: " + hostAddress.getHostZkNode());

    final Map<String, Runnable> adHocHealthCheckSchedule = scheduledMissingHostChecks.computeIfAbsent(
        hostAddress.getServiceKey(), sk -> new ConcurrentHashMap<>());

    adHocHealthCheckSchedule.computeIfAbsent(hostAddress.getHostUrl(), hostUrl -> {
      Runnable healthCheckNow = () -> {
        LOG.info("Starting ad-hoc health check for host: " + hostAddress.getHostZkNode());
        healthCheckServiceHost(hostAddress.getServiceKey(), hostAddress.getHostUrl()).thenApply((b) -> {
          // get rid of ourselves int the map, allowing others to schedule missing checks in the event that its not
          // really missing.. guard here from any cross thread additions
          synchronized (scheduledMissingHostChecks) {
            adHocHealthCheckSchedule.remove(hostUrl);
            if (adHocHealthCheckSchedule.isEmpty()) {
              LOG.info("No more missing reports for service: " + hostAddress.getServiceKey());
              scheduledMissingHostChecks.remove(hostAddress.getServiceKey());
            }
          }
          return b;
        });
      };

      executorService.submit(healthCheckNow);
      return healthCheckNow;
    });

  }

  /**
   * Reports a host missing for a service key. This doesn't necessarily result in a change in the mesh, it only serves
   * to schedule a host health check for the missing host in a faster manner than the cycling health check monitor. This
   * is a way for hosts to actively report the state to the mesh without polling often.
   *
   * @param serviceName   the service name
   * @param remoteHostUrl the remote host url
   *
   * @return true if host is reported missing
   */
  public CompletableFuture<Boolean> reportMissing(String serviceName, String remoteHostUrl) {
    HostAddress hostAddress = new HostAddress(zoolWriter.getZool().getServiceMapNode(), serviceName, remoteHostUrl);
    LOG.info("Zool Service Mesh recieved missing host report for hostAddress" + hostAddress);

    if (zoolServiceKey.equals(serviceName) && remoteHostUrl.equals(getLocalHostUrlAndPort(isProd(), -1))) {
      // skipping self
      debugIf(LOG, () -> "Skipping health check on self at " + remoteHostUrl);
      return CompletableFuture.completedFuture(false);
    }
    // check if we think we know about this host
    if (!meshNetwork.containsKey(serviceName)) {
      debugIf(LOG, () -> "Skipping health check, we dont know this service key: " + serviceName);
      // we don't know of this service don't health check
      return CompletableFuture.completedFuture(false);
    }

    Set<String> hostUrlsOnThisService = meshNetwork.get(serviceName);

    if (!hostUrlsOnThisService.contains(remoteHostUrl)) {
      // we don't know about this host, don't health check
      debugIf(LOG, () -> "Skipping health check, we dont know this service host: " + remoteHostUrl);
      return CompletableFuture.completedFuture(false);
    }

    // at this point we know about the host within our own service map, we'll report it missing
    Set<String> set = missingReports.computeIfAbsent(serviceName, sN -> new CopyOnWriteArraySet<>());
    // if we aren't already on the missing report

    if (set.add(remoteHostUrl)) {
      // schedule a health check. We'll remove it from the mesh and update the discovery services if it fails.
      scheduleHostHealthCheck(hostAddress);

      // return true even if its a repeat report.
      return CompletableFuture.completedFuture(true);
    } else {
      LOG.warn("Didn't schedule a health check for " + hostAddress);
      return CompletableFuture.completedFuture(false);
    }
  }

  private String serializedMeshNetwork() {
    String serializedMap;
    try {
      serializedMap = new ObjectMapper().writeValueAsString(meshNetwork);
    } catch (JsonProcessingException ex) {
      LOG.error("Could not serialize mesh network map", ex);
      serializedMap = "{}";
    }

    return serializedMap;
  }

  /**
   * Health checks a host, and cleans it if necessary
   *
   * @param serviceName   the service name
   * @param remoteHostUrl the host url
   *
   * @return a future of a boolean, with false if the host fails health check
   */
  private CompletableFuture<Boolean> healthCheckServiceHost(String serviceName, String remoteHostUrl) {
    HostAddress hostAddress = new HostAddress(zoolWriter.getZool().getServiceMapNode(), serviceName, remoteHostUrl);
    if (zoolServiceKey.equals(serviceName) && remoteHostUrl.equals(getLocalHostUrlAndPort(isProd(), -1))) {
      // skipping self
      debugIf(LOG, () -> "Skipping health check on self at " + remoteHostUrl);
      return CompletableFuture.completedFuture(true);
    }

    debugIf(LOG, () -> "Heath Check Service Host: " + serviceName + "/" + remoteHostUrl);
    RestRequest.Builder<Object, String> healthCheckBuilderRequestBuilder = new RestRequest.Builder<>(Object.class,
        String.class).setHost(hostAddress.getHost()).setPort(hostAddress.getPort());

    if (serviceName.equals(zoolServiceKey)) {
      debugIf(LOG, () -> "Sending GET health check to a known discovery service instance at: " + remoteHostUrl);
      // this is another dynamic discovery service instance, we will pass no data downstream
      healthCheckBuilderRequestBuilder.setRequestMethod(RequestMethod.GET);
    } else {
      debugIf(LOG, () -> "Sending POST health check to a remote discovery host instance at: " + remoteHostUrl);
      // we are checking a host instance, we'll pass our current knowledge of the system to the host since we get
      // ordered changes signaled from zookeeper already, we should have the latest data available since last signal
      // received.
      healthCheckBuilderRequestBuilder.setHeaders(ImmutableMap.of("Content-Type", "application/json"))
          .setRequestMethod(RequestMethod.POST)
          .setBody(serializedMeshNetwork());
    }

    RestRequest<Object, String> restRequest = healthCheckBuilderRequestBuilder.setRequestPath(
        discoveryHealthCheckEndpoint).build();

    return new StereoHttpTask<>(stereoHttpClient, serviceHealthCheckTimeout).execute(Object.class, restRequest)
        .thenApplyAsync(dynamicDiscoveryFeedback -> {
          debugIf(LOG, () -> "Discovery Health Check Response: " + dynamicDiscoveryFeedback.getStatus());
          boolean exists = true;
          if (dynamicDiscoveryFeedback.getStatus() != HttpStatus.SC_OK) {
            // suppress the host node!
            if (!zoolWriter.removeNode(hostAddress.getHostZkNode())) {
              // already removed
              LOG.warn("Already removed node: " + remoteHostUrl);
            } else {
              LOG.info("Removing remote node: " + remoteHostUrl);
            }

            // run a network healthcheck now
            networkHealthCheck();

            exists = false;
          } else {
            if (!zoolReader.nodeExists(hostAddress.getHostZkNode())) {
              LOG.warn("Host Node is not in zool!" + hostAddress.getHostZkNode());
            }
          }

          return exists;
        });
  }

  /**
   * Starts a cleaner thread for all hosts under a service
   *
   * @param latch          the latch
   * @param mutableHostSet the set of hosts to networkHealthCheck
   * @param serviceName    the service name
   *
   * @return a completable future.
   */
  private CompletableFuture<Void> startServiceHealthCheck(CountDownLatch latch,
      Set<String> mutableHostSet,
      String serviceName) {
    Set<String> hostSetCopy = ImmutableSet.copyOf(mutableHostSet);

    if (hostSetCopy.isEmpty()) {
      LOG.info("No hosts to networkHealthCheck for service " + serviceName);
      latch.countDown();
      return CompletableFuture.completedFuture(null);
    }

    LOG.info("Starting Health Check Requests for " + hostSetCopy.size() + " hosts");

    CompletableFuture[] hostCheckFutures = hostSetCopy.stream()
        .map(remoteHostUrl -> healthCheckServiceHost(serviceName, remoteHostUrl).thenApply(didExist -> {
          if (!didExist) {
            mutableHostSet.remove(remoteHostUrl);
          }
          return didExist;
        }))
        .toArray(CompletableFuture[]::new);

    // waiting on these
    return CompletableFuture.allOf(hostCheckFutures).thenRun(latch::countDown);
  }

  /**
   * Start the cleaner thread for the mesh network.
   */
  private void startNetworkHealthCheck() {
    LOG.info("Starting Mesh Network health check");
    Map<String, Set<String>> meshNetworkCopy = ImmutableMap.copyOf(getMeshNetwork());
    CountDownLatch cleanerLatch = new CountDownLatch(meshNetworkCopy.size());

    final long start = System.currentTimeMillis();
    final long intervalNow = serviceCleanScheduleInterval.getAndUpdate();
    meshNetworkCopy.forEach((serviceName, hostSet) -> {
      if (zoolServiceKey != null) {
        if (!hostSet.isEmpty()) {
          // one scheduled thread for each service
          scheduledExecutorService.schedule(() -> {
            startServiceHealthCheck(cleanerLatch, hostSet, serviceName).thenRun(
                () -> LOG.info("Completed health check for service " + serviceName + "!"));
          }, intervalNow, TimeUnit.MILLISECONDS);
        } else {
          LOG.info("No hosts on " + serviceName + " skipping....");
          cleanerLatch.countDown();
        }
      } else {
        cleanerLatch.countDown();
      }
    });

    try {
      cleanerLatch.await(cleanerLatch.getCount() * serviceHealthCheckTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while waiting for health check to complete...", ex);
    }

    LOG.info(
        "Service Mesh Network health check is complete. Total time: " + (System.currentTimeMillis() - start) + "ms");
  }

  /**
   * Override this to networkHealthCheck in your own way.
   *
   * @return a {@link CompletableFuture}
   */
  protected CompletableFuture<Void> networkHealthCheck() {
    healthCheckRunning = true;
    LOG.info("Network HealthCheck running on the mesh network map...");
    return CompletableFuture.runAsync(this::startNetworkHealthCheck, executorService)
        .thenRun(() -> healthCheckRunning = false);
  }

  /**
   * Announces a service host with no token. This will generate a {@link ZoolAnnouncement} with a new token :D.
   *
   * @param serviceKey service node name
   * @param hostUri    the host uri
   *
   * @return a {@link ZoolAnnouncement}
   */
  public ZoolAnnouncement announceServiceHost(final String serviceKey, String hostUri) {
    LOG.info("Announce Service Host -> " + serviceKey);
    return announceServiceHost(serviceKey, hostUri, null);
  }

  /**
   * Returns true if the service key and hostUri represent THIS host instance.
   *
   * @param serviceKey the service key
   * @param hostUri    the host uri
   *
   * @return a boolean, <code>true</code> if this is the host we're asking about.
   */
  public boolean isSelf(String serviceKey, String hostUri) {
    return zoolServiceKey.equals(serviceKey) && getLocalHostUrl(isProd()).equals(hostUri);
  }

  /**
   * Announces a service host with a token.
   *
   * @param serviceKey    the service key
   * @param hostUri       the host
   * @param existingToken an existing token (e.g. if it was staged)
   *
   * @return the {@link ZoolAnnouncement}
   */
  public ZoolAnnouncement announceServiceHost(final String serviceKey, String hostUri, String existingToken) {
    if (StringUtils.isEmpty(hostUri) || StringUtils.isEmpty(serviceKey)) {
      throw new IllegalStateException("Environment variables are not set for PROD or LOCAL server dns!");
    }

    ZoolAnnouncement announcement = new ZoolAnnouncement();
    announcement.currentEpochTime = System.currentTimeMillis();
    announcement.token = Optional.ofNullable(existingToken)
        .map(String::getBytes)
        .orElseGet(() -> getInstanceToken(hostUri));

    final String servicePath = ZConst.PathSeparator.ZK.join(getZoolReader().getZool().getServiceMapNode(), serviceKey);

    if (!zoolReader.nodeExists(servicePath) && !zoolWriter.createPersistentNode(servicePath)) {
      LOG.warn(
          "Could not create persistent service path node: " + servicePath + " it may have been created by " +
              "another" + " service mesh host");
    } else {
      LOG.info("Node " + servicePath + " exists or was created successfully");

      // if not already scheduled or running
      // we schedule one now to update hosts as soon as we can.
      if (!healthCheckRunning && !isSelf(serviceKey, hostUri) && !announcementChangeScheduled) {
        // run a health check of the network now
        announcementChangeScheduled = true;
        scheduledExecutorService.schedule(
            () -> this.networkHealthCheck().thenRun(() -> announcementChangeScheduled = false), 2000,
            TimeUnit.MILLISECONDS);
      }
    }

    meshNetwork.computeIfAbsent(serviceKey, zsk -> {
      LOG.warn("First announcement from services of type: " + serviceKey + ", path: " + servicePath);
      return new HashSet<>();
    }).add(hostUri);

    final String hostNodePath = ZConst.PathSeparator.ZK.join(servicePath, hostUri);

    infoIf(LOG, () -> "Announcing Service Host: " + hostNodePath);
    byte[] data = ZoolAnnouncement.serialize(announcement);

    if (data.length == 0 || !this.zoolWriter.createOrUpdateEphemeralNode(hostNodePath, data)) {
      LOG.error("Could create or update node: " + hostNodePath);
      announcement = null;
    } else {
      LOG.info("Created host node: " + hostNodePath + " with data: " + new String(announcement.token));
    }

    if (announcement != null) {
      infoIf(LOG, () -> "Announced host node: " + hostNodePath);
    }

    return announcement;
  }

  /**
   * The state of the Mesh
   */
  public enum MeshState {
    STOPPED, STARTING, RUNNING, STOPPING
  }
}
