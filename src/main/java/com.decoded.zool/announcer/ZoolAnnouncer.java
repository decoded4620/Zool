package com.decoded.zool.announcer;

import com.decoded.zool.ZoolAnnouncement;
import com.decoded.zool.ZoolServiceMesh;
import com.decoded.zool.ZoolSystemUtil;
import com.decoded.zool.models.ServiceGateway;
import com.decoded.zool.models.ServiceHost;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.decoded.zool.ZoolLoggingUtil.infoIf;


/**
 * This class represents the service between clients and zookeeper. This service will manage the announcements / hosts
 * as well as destroy nodes / services when hosts no longer are announcing.
 *
 * Bind / Inject this class after binding to ZoolServiceMesh
 */
public class ZoolAnnouncer {
  private static final Logger LOG = LoggerFactory.getLogger(ZoolAnnouncer.class);
  private final ZoolServiceMesh zoolServiceMesh;
  private final Map<String, Map<String, ServiceHost>> unprocessedAnnouncements = new ConcurrentHashMap<>();
  // everything we know that exists on zookeeper
  // we only need this to be a singleton executor, in order to move this off of the main thread.
  private final ScheduledExecutorService announcementExecutorService = Executors.newSingleThreadScheduledExecutor();

  // guarantee announcement availability within 3 seconds
  private int announcementProcessorInterval = 3000;
  private boolean processorEnqueued = false;

  @Inject
  public ZoolAnnouncer(ZoolServiceMesh zoolServiceMesh) {
    this.zoolServiceMesh = zoolServiceMesh;
  }

  /**
   * Set the interval (in milliseconds) to process announcements. This processor is only scheduled when announcements
   * are made
   *
   * @param announcementProcessorInterval the length of time to wait before processing announcements. E.g. in a very low
   *                                      volume server, this could be 0 where each announcement is directly written
   *                                      (because latency will be negligible). on a higher volume, this could be 1 to 5
   *                                      seconds, enough time to aggregate and predictably acquireGatewayHosts without creating
   *                                      backpressure.
   *
   * @return this announcer.
   */
  public ZoolAnnouncer setAnnouncementProcessorInterval(final int announcementProcessorInterval) {
    this.announcementProcessorInterval = announcementProcessorInterval;
    return this;
  }

  /**
   * Returns the zool service mesh, which knows the underlying network representation (for internal use).
   *
   * @return ZoolServiceMesh
   */
  public ZoolServiceMesh getZoolServiceMesh() {
    return zoolServiceMesh;
  }

  /**
   * Start the Zilli Service. You must call this prior to making it available to incoming traffic.
   *
   * @return CompletableFuture with the service mesh
   */
  public CompletableFuture<Map<String, Map<String, ZoolAnnouncement>>> start() {
    infoIf(LOG, () -> "ZoolAnnouncer start [secure: " + ZoolSystemUtil.isSecure() + "]");
    return this.zoolServiceMesh.start(ZoolSystemUtil.isSecure());
  }

  /**
   * Schedules the announcement queue processor when announcements come in. Synchronized for thread safety.
   */
  private void scheduleAnnouncementQueueProcessor() {
    if (!processorEnqueued) {
      infoIf(LOG, () -> "Enqueuing announcement processor for " + announcementProcessorInterval + " ms");
      processorEnqueued = true;
      // will happen in x ms
      this.announcementExecutorService.schedule(this::announcementQueueProcessor, announcementProcessorInterval,
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Stops the service, disconnecting from zool.
   */
  public synchronized void stop() {
    infoIf(LOG, () -> "Stopping Zool Announcer");
    if(processorEnqueued) {
      LOG.warn("Announcement processor is in flight and may not restart, severing the connection");
      processorEnqueued = false;
    }
    this.announcementExecutorService.shutdownNow();
  }

  /**
   * Starts the consumer for host announcements. This will poll the unprocessedAnnouncements map, and convert these into
   * Zookeeper Ephemeral nodes. Invoked off of the main thread to process incoming announcements.
   */
  private void announcementQueueProcessor() {
    infoIf(LOG, () -> "Announcement Queue Processor Running");
    // make this atomic

    Map<String, Map<String, ServiceHost>> fastMap = new HashMap<>();
    // lock the announcements while we clear the list and process them
    synchronized (unprocessedAnnouncements) {
      unprocessedAnnouncements.forEach(
          (k, v) -> v.forEach((token, host) -> fastMap.computeIfAbsent(k, tk -> new HashMap<>()).put(token, host)));
      unprocessedAnnouncements.clear();
    }

    // report each
    infoIf(LOG, () -> "Announcing " + fastMap.size() + " hosts...");

    fastMap.forEach((serviceName, serviceHostMap) -> serviceHostMap.forEach(
        (serviceToken, serviceHost) -> zoolServiceMesh.announceServiceHost(serviceName,
            serviceHost.getHostUrl() + ':' + serviceHost.getPort(), serviceHost.isSecure(), serviceToken)));

    processorEnqueued = false;
  }

  /**
   * Report Missing. This can be invoked by any host trying to reach another known network host. If that host returns a
   * 404, this can be called to schedule a health check on a single host.
   *
   * @param serviceKey    the service key
   * @param remoteHostUrl a host url
   *
   * @return the service map
   */
  public Map<String, ServiceGateway> reportMissing(String serviceKey, String remoteHostUrl) {
    infoIf(LOG, () -> "Report missing host " + remoteHostUrl + " for service " + serviceKey);
    // zool manages this automatically and can handle back pressure.
    // any instance reporting a missing host should only have to report once and it should choose a different host
    final Map<String, ServiceGateway> services = new HashMap<>();

    try {
      boolean reportSuccess = zoolServiceMesh.reportMissing(serviceKey, remoteHostUrl).thenApply(didReport -> {
        zoolServiceMesh.getMeshNetwork().forEach((k, x) -> {
          if (k.equals(serviceKey)) {
            List<ServiceHost> hosts = getHosts(k).stream()
                .filter(sh -> !remoteHostUrl.equals(sh.getHostUrl() + ':' + sh.getPort()))
                .collect(Collectors.toList());

            services.put(k, new ServiceGateway().setServiceKey(serviceKey).setHosts(hosts));
          } else {
            services.put(k, new ServiceGateway().setServiceKey(serviceKey).setHosts(getHosts(k)));
          }
        });

        return didReport;
      }).get();

      if (reportSuccess) {
        infoIf(LOG, () -> "Successfully reported " + remoteHostUrl + " missing for service: " + serviceKey);
      }
    } catch (InterruptedException ex) {
      infoIf(LOG, () -> "Could not report " + remoteHostUrl + " missing, interrupted while fetching mesh network: " +  ex.getMessage());
    } catch (ExecutionException ex) {
      infoIf(LOG, () -> "Could not report " + remoteHostUrl + " missing, execution exception while fetching mesh network: " +  ex.getMessage());
    }

    return services;
  }

  /**
   * Stages an announcement of a service host on the service gateway for a service key.
   *
   * @param serviceKey the service key.
   * @param hostUrl    the service host url.
   * @param hostPort   the service host port.
   *
   * @return the token created by gateway service for the announcer.
   */
  public String stageAnnouncement(String serviceKey, String hostUrl, int hostPort) {
    infoIf(LOG, () -> "Staging Zool Service Host Announcement for service: " + serviceKey + "@host[" + hostUrl + ":" + hostPort + "]");
    // returns the encoded bucket that your service / host was assigned to.
    String token = new String(ZoolServiceMesh.getInstanceToken(hostUrl + ':' + hostPort));

    Map<String, ServiceHost> announcements = unprocessedAnnouncements.computeIfAbsent(serviceKey,
        k -> new ConcurrentHashMap<>());

    ServiceHost announcement = new ServiceHost();
    announcement.setHostUrl(hostUrl).setPort(hostPort);

    if (announcements.containsKey(token)) {
      LOG.error("Cannot create announcement to " + serviceKey + "@[" + hostUrl + "] with supplied token, its already staged!");
    } else {
      // add any hosts that have announced, but not yet been processed.
      announcements.put(token, announcement);

      scheduleAnnouncementQueueProcessor();
    }
    return token;
  }

  /**
   * Returns the Client representation of the mesh network of hosts that are available on the network.
   *
   * @return a {@link Map} of {@link ServiceGateway} keyed by service key, each containing available hosts for a service.
   */
  public Map<String, ServiceGateway> getMeshNetwork() {
    Map<String, ServiceGateway> services = new HashMap<>();
    zoolServiceMesh.getMeshNetwork().forEach((k, x) -> services.put(k, new ServiceGateway().setServiceKey(k).setHosts(getHosts(k))));
    return services;
  }

  /**
   * Return the set of hosts for a service key.
   *
   * @param serviceKey a service key for hosts to select from.
   *
   * @return a list of {@link ServiceHost}
   */
  public List<ServiceHost> getHosts(String serviceKey) {
    Map<String, ZoolAnnouncement> hostsForService = zoolServiceMesh.getMeshNetwork().get(serviceKey);
    if (hostsForService == null) {
      return Collections.emptyList();
    }

    return hostsForService.keySet().stream().map(host -> {
      ZoolAnnouncement announcement = hostsForService.get(host);
      ServiceHost serviceHost = new ServiceHost().setToken(new String(announcement.token))
          .setSecure(announcement.securehost);
      int idx = host.indexOf(':');
      if (idx > -1) {
        return serviceHost.setHostUrl(host.substring(0, idx)).setPort(Integer.valueOf(host.substring(idx + 1)));
      } else {
        return new ServiceHost().setPort(80).setHostUrl(host);
      }
    }).collect(Collectors.toList());
  }
}
