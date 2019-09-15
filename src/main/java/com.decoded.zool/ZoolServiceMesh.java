package com.decoded.zool;

import com.decoded.javautil.Pair;
import com.decoded.stereohttp.RequestMethod;
import com.decoded.stereohttp.StereoHttpClient;
import com.decoded.stereohttp.StereoHttpRequest;
import com.decoded.stereohttp.StereoHttpTask;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
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

  private static final long MIN_TIME = 15000L;

  // these intervals start aggressive (small times) and get larger as they "ramp" up. each loop of the intervals
  // will grow these intervals until they "mature" to their max value.
  private static final ElasticInterval internalCleanInterval = ElasticInterval.elasticRamp(.15, MIN_TIME, 30000);
  private static final ElasticInterval serviceCleanScheduleInterval = ElasticInterval.elasticRamp(.10, MIN_TIME, 3000);
  private final StereoHttpClient stereoHttpClient;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Map<String, Map<String, ZoolAnnouncement>> meshNetwork = new ConcurrentHashMap<>();
  private final Map<String, Map<String, ZoolAnnouncement>> missingReports = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Runnable>> scheduledMissingHostChecks = new ConcurrentHashMap<>();
  private int serviceHealthCheckTimeout = 500;
  private boolean healthCheckRunning = false;
  private boolean healthCheckScheduled = false;
  private boolean isAnnounced = false;
  private String zoolServiceKey = "";
  private boolean isProd = false;
  private int port = -1;
  private ZoolWriter zoolWriter;
  private ZoolReader zoolReader;
  private boolean announcementChangeScheduled = false;
  private MeshState meshState = MeshState.STOPPED;
  private boolean secure = false;
  private ZoolConfig zoolConfig;

  @Inject
  public ZoolServiceMesh(ZoolConfig zoolConfig,
      ZoolReader zoolReader,
      ZoolWriter zoolWriter,
      ExecutorService executorService,
      ScheduledExecutorService scheduledExecutorService,
      StereoHttpClient stereoHttpClient) {
    this.zoolConfig = zoolConfig;
    this.zoolReader = zoolReader;
    this.zoolWriter = zoolWriter;
    this.executorService = executorService;
    this.scheduledExecutorService = scheduledExecutorService;
    this.stereoHttpClient = stereoHttpClient;
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
  public static Pair<String, ZoolAnnouncement> getNextHostFromHostAnnouncementList(AtomicLong hostIdxHolder,
      Map<String, ZoolAnnouncement> hosts) {
    if (hosts.isEmpty()) {
      LOG.warn("no hosts were supplied to getNextHostsFromHostList!");
      return new Pair<>("", new ZoolAnnouncement());
    }

    // corrected idx will always be within range
    String hostKey = new ArrayList<>(hosts.keySet()).get((int) (hostIdxHolder.getAndIncrement() % hosts.size()));

    return new Pair<>(hostKey, hosts.get(hostKey));
  }

  public static String getNextHostFromHostList(AtomicLong hostIdxHolder, Set<String> hosts) {
    if (hosts.isEmpty()) {
      LOG.warn("no hosts were supplied to getNextHostsFromHostList!");
      return "";
    }

    // corrected idx will always be within range
    return new ArrayList<>(hosts).get((int) (hostIdxHolder.getAndIncrement() % hosts.size()));
  }

  public boolean isSecure() {
    return secure;
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

  public ZoolConfig getZoolConfig() {
    return zoolConfig;
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
   * @param isSecure true if we are running in secure mode
   *
   * @return a completable future.
   */
  public CompletableFuture<Map<String, Map<String, ZoolAnnouncement>>> start(final boolean isSecure) {
    this.secure = isSecure;
    if (zoolServiceKey == null || zoolServiceKey.isEmpty()) {
      throw new IllegalStateException(
          "You must supply a zool service key for the service mesh. Here are some examples of good service mesh " +
              "keys:" + " " + "'discovery', 'gateway', or 'servicediscovery'.");
    }

    meshState = MeshState.STARTING;

    CompletableFuture<Map<String, Map<String, ZoolAnnouncement>>> serviceMeshFuture = new CompletableFuture<>();

    // the service map node
    zoolReader.readChannelAndChildren(zoolReader.getZool().getServiceMapNode(), (servicePath, bytes) -> {
      infoIf(LOG, () -> "Service Map at " + servicePath + " is available, waiting on children...");
    }, serviceMapPath -> {
      LOG.warn("Service Map at " + serviceMapPath + " is not available, creating it");
      if (!zoolWriter.createPersistentNode(serviceMapPath)) {
        LOG.error("Error creating: " + serviceMapPath);
      }
    }, (serviceMapPath, serviceNames) -> {
      infoIf(LOG, () -> "Service Map at: " + serviceMapPath + " loaded " + serviceNames.size() + " visible services");
      // if we were stopped prior to this point, bail
      // attempt to announce if children are found, but we're not in the list.
      // NOTE: We don't want to direct HTTPS traffic to non-https servers.
      this.announceSelfAsGateway(zoolServiceKey, isSecure);

      // update again
      updateHostsForAllServicesFromZool(serviceNames).thenAccept(voidT -> {
        if (stereoHttpClient.canStart()) {
          stereoHttpClient.start();
        }

        // initialize the network health check
        scheduleNetworkHealthCheck();

        serviceMeshFuture.complete(getMeshNetwork());
      });

      meshState = MeshState.RUNNING;

    }, serviceMapPath -> {
      // if no children are found, we'll announce ourselves now
      this.announceSelfAsGateway(zoolServiceKey, isSecure);
      meshState = MeshState.RUNNING;
      serviceMeshFuture.complete(getMeshNetwork());
    });

    infoIf(LOG, () -> "Connecting to Zool..");

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
  public Map<String, Map<String, ZoolAnnouncement>> getMeshNetwork() {
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
        serviceKeys.forEach(serviceKey -> {
          // inner jobs?
          final String servicePath = ZConst.PathSeparator.ZK.join(getZoolReader().getZool().getServiceMapNode(),
              serviceKey);

          if (!zoolReader.isReading(servicePath)) {
            zoolReader.readChildren(servicePath, (sp, hostList) -> {
              infoIf(LOG, () -> "Loading " + hostList.size() + " hosts from service path: " + sp);

              hostList.forEach(hostKey -> {
                debugIf(LOG, () -> "Reading host data: " + hostKey);
                // read the zookeeper data
                zoolReader.readChannel(ZConst.PathSeparator.ZK.join(servicePath, hostKey), (channelPath, bytes) -> {
                  infoIf(LOG, () -> "Service " + channelPath + " was found with " + bytes.length + " bytes");
                  ZoolAnnouncement announcement = ZoolAnnouncement.deserialize(bytes);
                  if (announcement.token.length > 0) {
                    Map<String, ZoolAnnouncement> announcements = meshNetwork.computeIfAbsent(serviceKey,
                        x -> new HashMap<>());
                    long existingAnnouncementsForKey = announcements.keySet()
                        .stream()
                        .filter(key -> key.equals(serviceKey))
                        .count();
                    if (existingAnnouncementsForKey == 0) {
                      announcements.put(hostKey, announcement);
                    } else {
                      infoIf(LOG, () -> "Announcement: " + hostKey + " was already made");
                    }
                  } else {
                    LOG.error("Zilli announcement was invalid, no token was included");
                  }
                }, keyNoData -> {
                  infoIf(LOG, () -> "Key " + keyNoData + " was read, but no data was inside!");
                });
              });
              infoIf(LOG, () -> "Updated mesh for " + serviceKey + " to " + hostList.size() + " total hosts");
            }, p -> LOG.warn("No hosts found on service path " + p));
          } else {
            LOG.warn("service path " + servicePath + ", is already being read");
          }
        });

        int totalHosts = meshNetwork.values().stream().mapToInt(Map::size).sum();
        infoIf(LOG,
            () -> "Loaded Total Services: " + meshNetwork.size() + " services and " + totalHosts + " total hosts");
        debugIf(LOG, () -> "-------------------------------------------------------------");
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
   * @param isSecure       true if we are announcing as a secure host.
   */
  protected void announceSelfAsGateway(final String zoolGatewayKey, final boolean isSecure) {
    if (!this.isAnnounced) {
      infoIf(LOG, () -> "Self Announcing Service mesh host on service key: " + zoolGatewayKey);
      ZoolAnnouncement zoolGatewayAnnouncement = announceServiceHost(zoolGatewayKey,
          getLocalHostUrlAndPort(isProd, isSecure, zoolConfig), isSecure);

      if (zoolGatewayAnnouncement == null || zoolGatewayAnnouncement.token.length == 0) {
        LOG.error("Announcement failure", new ZoolServiceException("Announcement was a failure"));
      } else {
        isAnnounced = true;
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
   * @param remoteHostAddress a RemoteHostAddress object to health check
   */
  private void scheduleHostHealthCheck(final RemoteHostAddress remoteHostAddress) {
    LOG.info("Scheduling an ad-hoc health check for host node: " + remoteHostAddress.getHostZkNode());

    final Map<String, Runnable> adHocHealthCheckSchedule = scheduledMissingHostChecks.computeIfAbsent(
        remoteHostAddress.getServiceKey(), sk -> new ConcurrentHashMap<>());

    adHocHealthCheckSchedule.computeIfAbsent(remoteHostAddress.getHostUrl(), hostUrl -> {
      Runnable healthCheckNow = () -> {
        LOG.info("Starting ad-hoc health check for host: " + remoteHostAddress.getHostZkNode());

        Map<String, ZoolAnnouncement> serviceHostMap = meshNetwork.get(remoteHostAddress.getServiceKey());
        // get the announcement for this host
        Optional<String> maybeAnnouncementId = serviceHostMap.keySet()
            .stream()
            .filter(key -> key.equals(hostUrl))
            .findFirst();

        if (!maybeAnnouncementId.isPresent()) {
          LOG.info("No Announcement for " + hostUrl);
        }

        maybeAnnouncementId.ifPresent(
            announcementId -> healthCheckServiceHost(remoteHostAddress.getServiceKey(), announcementId,
                serviceHostMap.get(announcementId)).thenAccept((b) -> {
              // get rid of ourselves in the map, allowing others to schedule missing checks in the event that its not
              // really missing.. guard here from any cross thread additions
              synchronized (scheduledMissingHostChecks) {
                adHocHealthCheckSchedule.remove(hostUrl);
                if (adHocHealthCheckSchedule.isEmpty()) {
                  LOG.info("No more missing reports for service: " + remoteHostAddress.getServiceKey());
                  scheduledMissingHostChecks.remove(remoteHostAddress.getServiceKey());
                }
              }
            }));
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
    RemoteHostAddress remoteHostAddress = new RemoteHostAddress(zoolWriter.getZool().getServiceMapNode(), serviceName, remoteHostUrl);
    LOG.info("Zool Service Mesh received missing host report for remoteHostAddress" + remoteHostAddress);

    if (zoolServiceKey.equals(serviceName) && remoteHostUrl.equals(
        getLocalHostUrlAndPort(isProd(), ZoolSystemUtil.isSecure(), zoolConfig))) {
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

    Map<String, ZoolAnnouncement> hostUrlAndDataPairsOnThisServiceMesh = meshNetwork.get(serviceName);

    Optional<ZoolAnnouncement> matchedHost = hostUrlAndDataPairsOnThisServiceMesh.keySet()
        .stream()
        .filter(key -> key.equals(remoteHostUrl))
        .map(hostUrlAndDataPairsOnThisServiceMesh::get)
        .findFirst();

    if (!matchedHost.isPresent()) {
      // we don't know about this host, don't health check
      debugIf(LOG, () -> "Skipping health check, we dont know this service host: " + remoteHostUrl);
      return CompletableFuture.completedFuture(false);
    } else {

      // at this point we know about the host within our own service map, we'll report it missing
      Map<String, ZoolAnnouncement> set = missingReports.computeIfAbsent(serviceName, sN -> new ConcurrentHashMap<>());
      // if we aren't already on the missing report

      matchedHost.ifPresent(host -> {
        set.put(remoteHostUrl, host);
        // schedule a health check. We'll remove it from the mesh and update the discovery services if it fails.
        scheduleHostHealthCheck(remoteHostAddress);

      });
      // return true even if its a repeat report.
      return CompletableFuture.completedFuture(true);
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
   * Remove a host address from our service mesh
   *
   * @param remoteHostAddress the {@link RemoteHostAddress}
   */
  private void removeHostAddress(RemoteHostAddress remoteHostAddress) {
    infoIf(LOG, () -> "Remove Host Address: " + remoteHostAddress);
    // suppress the host node!
    if (!zoolWriter.removeNode(remoteHostAddress.getHostZkNode())) {
      // already removed
      LOG.warn("Not not removed: " + remoteHostAddress.getHostUrl());
    }

    // run a network health check now
    networkHealthCheck();
  }

  /**
   * Health checks a host, and cleans it if necessary
   *
   * @param serviceName    the service name
   * @param remoteHostUrl  the host url
   * @param remoteHostData The announcement data
   *
   * @return a future of a boolean, with false if the host fails health check
   */
  private CompletableFuture<Boolean> healthCheckServiceHost(String serviceName,
      String remoteHostUrl,
      ZoolAnnouncement remoteHostData) {

    RemoteHostAddress remoteHostAddress = new RemoteHostAddress(zoolWriter.getZool().getServiceMapNode(), serviceName, remoteHostUrl);
    final String localHostUrlAndPort = getLocalHostUrlAndPort(isProd(), ZoolSystemUtil.isSecure(), zoolConfig);

    if (zoolServiceKey.equals(serviceName) && remoteHostUrl.equals(localHostUrlAndPort)) {
      // skipping self
      debugIf(LOG, () -> "Skipping health check on self at " + remoteHostUrl);
      return CompletableFuture.completedFuture(true);
    }

    debugIf(LOG, () -> "Heath Check Service Host: " + serviceName + "/" + remoteHostUrl);
    StereoHttpRequest.Builder<Object, String> healthCheckBuilderRequestBuilder = new StereoHttpRequest.Builder<>(
        Object.class, String.class).setSecure(remoteHostData.securehost)
        .setHost(remoteHostAddress.getHost())
        .setPort(remoteHostAddress.getPort());

    if (serviceName.equals(zoolServiceKey)) {
      debugIf(LOG, () -> "Sending GET health check to a known discovery service instance at: " + remoteHostUrl);
      // this is another dynamic discovery service instance, we will pass no data downstream
      healthCheckBuilderRequestBuilder.setRequestMethod(RequestMethod.GET);
    } else {
      debugIf(LOG, () -> "Sending POST health check to a remote discovery host instance at: " + remoteHostUrl);
      // we are checking a host instance, we'll pass our current knowledge of the system to the host since we get
      // ordered changes signaled from zookeeper already, we should have the latest data available since last signal
      // received.
      healthCheckBuilderRequestBuilder.addHeader("Content-Type", "application/json")
          .setRequestMethod(RequestMethod.POST)
          .setBody(serializedMeshNetwork());
    }

    StereoHttpRequest<Object, String> stereoHttpRequest = healthCheckBuilderRequestBuilder.setRequestPath(
        zoolConfig.discoveryHealthCheckEndpoint).build();

    return new StereoHttpTask<>(stereoHttpClient, serviceHealthCheckTimeout).execute(Object.class, stereoHttpRequest)
        .thenApplyAsync(dynamicDiscoveryFeedback -> {
          debugIf(LOG, () -> "Discovery Health Check Response: " + dynamicDiscoveryFeedback.getStatus());
          boolean exists = true;
          if (dynamicDiscoveryFeedback.getStatus() != HttpStatus.SC_OK) {
            infoIf(LOG,
                () -> "Host " + remoteHostAddress + " doesn't respond from healthcheck after " + serviceHealthCheckTimeout + " ms");
            removeHostAddress(remoteHostAddress);
            exists = false;
          } else {
            if (!zoolReader.nodeExists(remoteHostAddress.getHostZkNode())) {
              LOG.warn("Host Node is not in zool!" + remoteHostAddress.getHostZkNode());
            }
          }

          return exists;
        });
  }

  /**
   * Starts a cleaner thread for all hosts under a service
   *
   * @param latch          the latch that will countdown when host check completes.
   * @param mutableHostSet the set of hosts to networkHealthCheck
   * @param serviceName    the service name
   *
   * @return a completable future.
   */
  private CompletableFuture<Void> startServiceHealthCheck(CountDownLatch latch,
      Map<String, ZoolAnnouncement> mutableHostSet,
      String serviceName) {
    Map<String, ZoolAnnouncement> hostSetCopy = ImmutableMap.copyOf(mutableHostSet);

    if (hostSetCopy.isEmpty()) {
      infoIf(LOG, () -> "No hosts to networkHealthCheck for service " + serviceName);
      latch.countDown();
      return CompletableFuture.completedFuture(null);
    }

    infoIf(LOG, () -> "Starting Health Check Requests for " + hostSetCopy.size() + " hosts");

    CompletableFuture[] hostCheckFutures = hostSetCopy.keySet()
        .stream()
        .map(remoteHostUrl -> healthCheckServiceHost(serviceName, remoteHostUrl,
            hostSetCopy.get(remoteHostUrl)).thenApply(didExist -> {
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
    infoIf(LOG, () -> "Starting Mesh Network health check");
    Map<String, Map<String, ZoolAnnouncement>> meshNetworkCopy = getMeshNetwork();
    CountDownLatch cleanerLatch = new CountDownLatch(meshNetworkCopy.size());

    final long start = System.currentTimeMillis();
    final long intervalNow = serviceCleanScheduleInterval.getAndUpdate();
    meshNetworkCopy.forEach((serviceName, hostSet) -> {
      if (zoolServiceKey != null) {
        if (!hostSet.isEmpty()) {
          // one scheduled thread for each service
          scheduledExecutorService.schedule(() -> {
            startServiceHealthCheck(cleanerLatch, hostSet, serviceName).thenRun(
                () -> infoIf(LOG, () -> "Completed health check for service " + serviceName + "!"));
          }, intervalNow, TimeUnit.MILLISECONDS);
        } else {
          infoIf(LOG, () -> "No hosts on " + serviceName + " skipping....");
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

    infoIf(LOG,
        () -> "Service Mesh Network health check is complete. Total time: " + (System.currentTimeMillis() - start) +
            "ms");
  }

  /**
   * Override this to networkHealthCheck in your own way.
   *
   * @return a {@link CompletableFuture}
   */
  protected CompletableFuture<Void> networkHealthCheck() {
    healthCheckRunning = true;
    infoIf(LOG, () -> "Network HealthCheck running on the mesh network map...");
    return CompletableFuture.runAsync(this::startNetworkHealthCheck, executorService)
        .thenRun(() -> healthCheckRunning = false);
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
   * Create the service node, or if it already exists, schedule a health check now to broadcast the new node being
   * announced. This method cannot be executed in parallel.
   *
   * @param servicePath the service path
   * @param serviceKey  service key
   * @param hostUri     the host uri being added.
   */
  private synchronized boolean broadcastAnnouncement(String servicePath, String serviceKey, String hostUri) {
    if (!zoolReader.nodeExists(servicePath) && !zoolWriter.createPersistentNode(servicePath)) {
      LOG.warn(
          "Could not create persistent service path node: " + servicePath + " it may have been created by " +
              "another" + " service mesh host");
      return false;
    } else {
      infoIf(LOG, () -> "Node " + servicePath + " exists or was created successfully");

      // if not already scheduled or running
      // we schedule one now to update hosts as soon as we can.
      if (!healthCheckRunning && !isSelf(serviceKey, hostUri) && !announcementChangeScheduled) {
        // run a health check of the network now
        announcementChangeScheduled = true;
        scheduledExecutorService.schedule(
            () -> this.networkHealthCheck().thenRun(() -> announcementChangeScheduled = false), 2000,
            TimeUnit.MILLISECONDS);
        return true;
      }
      return false;
    }
  }

  /**
   * Announces a service host with no token. This will generate a {@link ZoolAnnouncement} with a new token :D.
   *
   * @param serviceKey service node name
   * @param hostUri    the host uri
   * @param isSecure   true if this is a secure host.
   *
   * @return a {@link ZoolAnnouncement}
   */
  public ZoolAnnouncement announceServiceHost(final String serviceKey, String hostUri, boolean isSecure) {
    infoIf(LOG, () -> "Announce Service Host -> " + serviceKey);
    return announceServiceHost(serviceKey, hostUri, isSecure, null);
  }

  /**
   * Announces a service host with a token.
   *
   * @param serviceKey    the service key
   * @param hostUri       the host
   * @param isSecureHost  true if the host is secure.
   * @param existingToken an existing token (e.g. if it was staged)
   *
   * @return the {@link ZoolAnnouncement}
   */
  public ZoolAnnouncement announceServiceHost(final String serviceKey,
      String hostUri,
      boolean isSecureHost,
      String existingToken) {
    infoIf(LOG, () -> "Announcing service host: " + serviceKey + ", " + hostUri + ": " + existingToken);

    if (StringUtils.isEmpty(hostUri) || StringUtils.isEmpty(serviceKey)) {
      throw new IllegalStateException("Environment variables are not set for PROD or LOCAL server dns!");
    }

    ZoolAnnouncement announcement = new ZoolAnnouncement();
    announcement.currentEpochTime = System.currentTimeMillis();
    announcement.securehost = isSecureHost;
    announcement.token = Optional.ofNullable(existingToken)
        .map(String::getBytes)
        .orElseGet(() -> ZoolSystemUtil.getInstanceToken(hostUri));

    final byte[] data = ZoolAnnouncement.serialize(announcement);
    final String servicePath = ZConst.PathSeparator.ZK.join(getZoolReader().getZool().getServiceMapNode(), serviceKey);
    final boolean didBroadcast = broadcastAnnouncement(servicePath, serviceKey, hostUri);
    final String hostNodePath = ZConst.PathSeparator.ZK.join(servicePath, hostUri);

    infoIf(LOG, () -> (didBroadcast
        ? "Broadcast announcement: "
        : "Skipped broadcast: ") + servicePath + " @[" + hostUri + "]");

    if (data.length == 0 || !this.zoolWriter.createOrUpdateEphemeralNode(hostNodePath, data)) {
      LOG.error("Could create or update node: " + hostNodePath);
      announcement = null;
    } else {
      // add the host before the network call to make its availability that much faster
      meshNetwork.computeIfAbsent(serviceKey, zsk -> {
        LOG.warn("First announcement from : " + zsk + " services @" + servicePath);
        return new HashMap<>();
      }).put(hostUri, announcement);

      infoIf(LOG, () -> "Created host node: " + hostNodePath);
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
