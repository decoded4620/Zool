# Zool

<img src="./docs/images/zuul.jpg" width="60%">

Zool is an interface for synchronizing live configuration using Apache Zookeeper.

## JavaDocs
Read them [Here](https://decoded4620.github.io/Zool/docs/javadoc/)

## Implementing a Dynamic Discovery Service Container using Zool

### Main Server Module Example

#### Configure your Zool Service Host

This example is using a Hocon Style Application Configuration but Zool is completely unopinionated on your configuration setup. This can be approached by any means.

```
hocon {
  # Stereo Http Configuration
  httpClient {
    maxOutboundConnections=5
    maxOutboundConnectionsPerRoute=5
  }

  # Zookeeper Configuration
  zookeeper {
    client {
      zkConnectTimeout=10000
      zkHost="127.0.0.1"
      zkPort=2181
      zkServiceMapNode="/services"
    }
  }
}
```


#### Create a custom Stereo Http Client
Stereo Http is how Zool communicates with external services. It is an Apache NIO based http client.
The main demonstration shown is setting up the maxOutboundConnections and maxOutboundConnectionsPerRoute properties of the [Stereo Http Client](http://github.com/decoded4620/StereoHttp). The client doesn't have to be extended, but this is a convenient way to inject your own configuration, in this exmaple, Hocon Style Configuration from Play
```java
/**
 * Custom Extension of Stereo Http Client
 */
public class MyOwnStereoHttpClient extends StereoHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(MyOwnStereoHttpClient.class);
  private Cfg cfg;

  @Inject
  public MyOwnStereoHttpClient(ExecutorService executorService) {
    super(executorService);
    

    // TODO create your Cfg instance here using your own means (e.g. play Hocon Configuration, or other)

    setMaxOutboundConnectionsPerRoute(cfg.maxOutboundConnectionsPerRoute);
    setMaxOutboundConnections(cfg.maxOutboundConnections);
  }

  public static class Cfg {
    public int maxOutboundConnections = 100;
    public int maxOutboundConnectionsPerRoute = 30;
  }
}
```

#### Create a custom DynamicDiscovery Server Module
Note: This is using examples from Play Framework, however, there is no requirement that your zool service must be a Play Service. This is just an example to get up and running.
Zool does however use Guice Injection (which is operable with Java's builtin in `@Inject` mechanisms). In these examples, we're using the Abstract Module to perform bindings from Zool Interfaces to
their implementations.

```java
public class DynamicDiscoveryServiceModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicDiscoveryServiceModule.class);

  @Override
  protected void configure() {
    // -----------------
    // Http client using a custom configured client. The custom class is shown below, and is only used
    // to ingest Hocon Configuration from the Container Application (in this case the example is play)
    bind(StereoHttpClient.class).to(MyOwnStereoHttpClient.class).asEagerSingleton();

    // Handles host network and debugging data.
    bind(HostDataProvider.class).asEagerSingleton();
    // setup scheduled executor service
    bind(ScheduledExecutorService.class).toInstance(Executors.newScheduledThreadPool(10));
    // setup executor services
    bind(ExecutorService.class).toInstance(Executors.newFixedThreadPool(10));
    // -----------------
    // Zookeeper
    bind(ZoolDataFlow.class).to(ZoolDataFlowImpl.class).asEagerSingleton();
    bind(Zool.class).to(Zilli.class).asEagerSingleton();
    bind(ZoolServiceMeshClient.class).to(ZilliClient.class).asEagerSingleton();
    // -----------------
    // ZoolServiceHub
    bind(ZoolServiceMesh.class).asEagerSingleton();
  }
}
```

Given a similar setup to the above, your Play Module now becomes a Zool Enabled Service Module. If you are running a Zookeeper Server at the specified ip / port in the zookeeper.client configuration path shown above

### Implement an Application Container or similar object
In Play Framework Style apps, we can implement application containers which are startup and shutdown aware. This means that when Play Framework starts or stops, they provide hooks in order to handle the events.

```java
package com.my.app.container;

import com.decoded.polldancer.PollDancer;
import com.decoded.stereohttp.StereoHttpClient;
import com.google.inject.Inject;
import play.Logger;
import play.api.Play;
import play.inject.ApplicationLifecycle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;


/**
 * This class represents the main web container, and its connection to the orchestrated system that is milli
 * microservices.
 * This system includes zookeeper, and several service pools, each host an instance of the Milli Application Container.
 * <p>
 * The container can announce to zookeeper, and detect available zookeeper services on the same Quorum.
 * <p>
 * The container provides access to an Http Client ({@link StereoHttpClient}) which can be used to make requests to
 * webservices,
 * milli micro services, or any http accessible Url.
 */
@SuppressWarnings("WeakerAccess")
public class ApplicationContainer {

  private static final Logger.ALogger LOG = Logger.of(ApplicationContainer.class);

  private final Cfg cfg;
  private final ExecutorService executorService;

  private final ApplicationLifecycle lifecycle;
  private StereoHttpClient stereoHttpClient;

  @Inject
  public ApplicationContainer(
                              ModuleConfiguration moduleConfiguration,
                              ExecutorService executorService,
                              StereoHttpClient httpClient,
                              ApplicationLifecycle lifecycle,
                              ZoolServiceMesh zoolService
  ) {
    LOG.info("Milli Application is starting...");
    
    // TODO setup Cfg instance using Hocon or other means.. 

    this.lifecycle = lifecycle;
    this.moduleConfiguration = moduleConfiguration;
    this.executorService = executorService;
    this.stereoHttpClient = httpClient;
    this.zoolService = zoolService;

    startHttpClient();
  }

  /**
   * Get the conf configuration for the container (play application uses Hocon)
   * @return the hocon configuration
   */
  public Cfg getCfg() {
    return cfg;
  }

  /**
   * Start the Http Client.
   */
  protected void startHttpClient() {
    if (stereoHttpClient.canStart()) {
      LOG.info("Starting http client on thread: " + Thread.currentThread().getName());
      stereoHttpClient.start();
    }
  }

  /**
   * Container Configuration
   */
  public static class Cfg {
    /**
     * This value controls the name for the process under zookeeper barrier node for this api. Each host will have
     * its own e.g.
     * barrierNode/services/myService_192.168.0.1
     */
    public String zookeeperServiceKey = "";

    /**
     * The zookeeper gateway service key for ping / announce / updating dd hosts.
     */
    public String zookeeperGatewayKey = "";

    /**
     * Production flag.
     */
    public boolean isProd = false;
  }
}

```
### Implement a Zool Client
You can Implement your own Zool Client simply by extending the Zool Abstract

```java
package your.cool.discoveryservice.ZoolClient;

import com.decoded.zool.Zool;
import com.decoded.zool.ZoolDataFlow;

import javax.inject.Inject;

/**
 * Your Cool App ZoolClient
 */
public class ZoolClient extends Zool {

  @Inject
  public ZoolClient(ZoolDataFlow zoolDataFlow) {
    super(zoolDataFlow);
 
    setHost("localhost");
    setPort(2181);
    setTimeout(10000);
    setServiceMapNode("/services");
    setGatewayMapNode("/gateway");
  }
}
```
### Start Interacting with your Zookeeper Server
```java
Zool zoolClient;

final String serviceMapNode = this.zoolClient.getServiceMapNode();
final String gatewayNode = this.zoolClient.getGatewayMapNode();

List<ZoolDataSink> dataHandlers = ImmutableList.of(
    new ZoolDataSinkImpl(gatewayNode, this::onGatewayData, this::onGatewayNoData),
    new ZoolDataSinkImpl(serviceMapNode, this::onZoolServicesData, this::onZoolServicesNoData)
);

dataHandlers.forEach(this.zoolClient::drain);

this.zoolClient.connect();
``` 

### Handle Data (or Not)

```java
private void onGatewayData(String path, byte[] data) {
  
}

private void onGatewayNoData(String path) {
  throw new IllegalStateException("No gateway data was found");
}

private void onZoolServicesData(String path, byte[] data) {
  List<String> children = zkClient.getChildren(path);
   
  System.out.println("path: " + path + ", data: " + data.length);
  children.forEach(childName -> {
    System.out.println(path + '/' + childName);
  });
}

private void onZoolServicesNoData(String path) {
}
```
## Zool Service Hub
Zool Service Hub is a wrapper around Zool which allows a container application to become aware of the zookeeper network by announcing itself, and also getting a copy of the other hosts that have already announced. Each zookeeperHost connected to the same Zookeeper quarum gets a copy of the entire map.  

The ServiceHub can be Injected with JavaX or Guice injection and requires a Zool instance, and an ExecutorService
```java
ZoolAnnouncerHub
bind(ZoolServiceHub.class).asEagerSingleton();
```
Then you can set it up like so:
```java
@Inject
public YourClass(ZoolServiceHub microServicesHub) {
...
}

// zk port
microServicesHub.setPort(2181);

// the service key that this zookeeperHost will live in
microServicesHub.setServiceKey("userService");
// how often to get updates from zk
microServicesHub.setPollingInterval(2000);
// if production true, false for dev
microServicesHub.setProd(true);
// this will start the hub, and announce our local url, as well as exchange for a copy of the other
// services on the network.
microServicesHub.start();
```

You can later stop the hub:
```java
// stops the service hub (unannouncing effectively your services zookeeperHost)
microServicesHub.stop();
```
To get known services based on the gateway service map and service keys:

```java
  List<String> knownServiceKeys = microServicesHub.getKnownServices();
  List<String> hostsForUserService = getHostsForService("userService");
  
  // keys will be something similar to { "userService" }
  // hosts for userService would be something such as { "localhost:9001", "localhost:9002" }
```
### Service and Gateway Nodes
The ZoolServiceHub will automatically create the gateway and service map nodes based on the
configuration. You can create several service maps and gateways in a zookeeper network to control
the sharing of information for example.
```
serviceMap0
 |- loginService
 |--- localhost:9443
serviceMap1
 |- userService
 |--- localhost:9001
 |--- localhost:9003
 |- activityService
 |--- localhost:9002
serviceMap2
 |- adminService
 |--- localhost:9004
 |- financeService
 |--- localhost:9005
 |--- localhost:9006
```
To overlap, you might want to share the login service above between the userService and the adminService in some way.


Both user service and admin service provide an additional ZoolServiceHub instance, pointing to "serviceMap0" as its gateway.

NOTE: This particular system is still under consideration!

### Creating your service and gateway nodes manually
This assumes you have Zookeeper installed on your Linux, Mac, or Windows system.
1. Run your Zookeeper Server (e.g. localhost:2181)
2. Open zkCli.sh (or zkCli.bat for windows)
Once you see

```[zk: localhost:2181(CONNECTED) 0]```

You can create a `services` and `gateway` node.

```
[zk: localhost:2181(CONNECTED) 0] create /gateway {}
Created /gateway
[zk: localhost:2181(CONNECTED) 1]

[zk: localhost:2181(CONNECTED) 0] create /services {}
Created /services
[zk: localhost:2181(CONNECTED) 1]
```

Then check the directory of the zookeeper root
```
[zk: localhost:2181(CONNECTED) 2] ls /
[services, zookeeper, gateway]
[zk: localhost:2181(CONNECTED) 3]
```

