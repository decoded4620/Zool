# Zool

<img src="./docs/images/zuul.jpg" width="60%">

Zool is an interface for synchronizing live configuration using Apache Zookeeper.

## JavaDocs
Read them [Here](https://decoded4620.github.io/Zool/docs/javadoc/)

## Implementing a Dynamic Discovery Service Container using Zool

### Main Server Module Example

#### Configure your Zool Service Host

This example is using a Hocon Style Application Configuration but Zool is completely unopinionated on your configuration setup. This can be approached by any means.

example `application.conf` contents for localhost development
```
servers {
  localhost="127.0.0.1"
  zkPort=2181
  zookeeper = ${servers.localhost}
}

# Zookeeper Configuration
discovery {
# Zookeeper Configuration
zookeeper {
  configuration {
      ######################################################
      # NGROK flag allows us to bypass the port value when
      # Announcing to Zool since ngrok hides the port value
      # (By default its  80)
      ######################################################
      usengrok=true
      discoveryHealthCheckEndpoint="/discoveryHealthCheck"
  }
  connection {
    zkConnectTimeout=10000
    zkHost=${servers.zookeeper}
    zkPort=${servers.zkPort}
    zkServiceMapNode="/services"
  }
}
```


#### Create a custom Stereo Http Client
`StereoHttp` the mechanism used by Zool to communicate with external services over `http`/`https`. It is an Apache NIO based http client.
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
    // Zool Bindings
    bind(ZoolConfig.class).to(MilliZoolConfiguration.class).asEagerSingleton();
    bind(ZookeeperConnection.class).to(MilliZookeeperConnection.class).asEagerSingleton();
    bind(ZoolDataFlow.class).to(ZoolDataFlowImpl.class).asEagerSingleton();
    bind(Zool.class).to(Zilli.class).asEagerSingleton();
    bind(ZilliClient.class).asEagerSingleton();
    bind(ZoolServiceMeshClient.class).to(ZilliClient.class).asEagerSingleton();
    bind(ZoolServiceMesh.class).asEagerSingleton();
  }
}
```

Given a similar setup to the above, your Play Module now becomes a Zool Enabled Service Module. If you are running a Zookeeper Server at the specified ip / port in the zookeeper.client configuration path shown above

### Implement an Application Container or similar object
In Play Framework Style apps, we can implement application containers which are startup and shutdown aware. This means that when Play Framework starts or stops, they provide hooks in order to handle the events.

```java
package com.milli.container;

import com.decoded.polldancer.PollDancer;
import com.decoded.stereohttp.StereoHttpClient;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.milli.container.configuration.ContainerConfiguration;
import com.milli.core.configuration.ConfigScope;
import play.Logger;
import play.api.Application;
import play.api.Play;
import play.inject.ApplicationLifecycle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@SuppressWarnings("WeakerAccess")
public class ApplicationContainer {
  private static final ConfigScope CFG_SCOPE = ContainerScopes.CONTAINER.child("applicationContainer");
  
  private final Cfg cfg;
  private final ExecutorService executorService;
  private final ContainerConfiguration containerConfiguration;
  private final ModuleConfiguration moduleConfiguration;
  private final ApplicationLifecycle lifecycle;
  private StereoHttpClient stereoHttpClient;
  private Provider<Application> applicationProvider;
  
  @Inject
  public ApplicationContainer(Provider<Application> applicationProvider, ContainerConfiguration containerConfiguration,
                              ModuleConfiguration moduleConfiguration,
                              ExecutorService executorService,
                              StereoHttpClient httpClient,
                              ApplicationLifecycle lifecycle
  ) {
    this.containerConfiguration = containerConfiguration;
    this.cfg = this.containerConfiguration.maybeScopedConfiguration(CFG_SCOPE, Cfg.class).orElseGet(Cfg::new);
    this.applicationProvider = applicationProvider;
    this.lifecycle = lifecycle;
    this.moduleConfiguration = moduleConfiguration;
    this.executorService = executorService;
    this.stereoHttpClient = httpClient;

    waitForAppStart();
  } 
  
  public Cfg getCfg() {
    return cfg;
  } 
  
  public ContainerConfiguration getContainerConfiguration() {
    return containerConfiguration;
  }
 
  public ModuleConfiguration getModuleConfiguration() {
    return moduleConfiguration;
  }

  private void waitForAppStart() {
    lifecycle.addStopHook(this::onAppShutdown);
    // play poll dancer is triggered when Play Current is available.
    new PollDancer(executorService, this::onAppStartup).setPollTrigger(() -> {
      try {
        Play.current();
        return true;
      } catch (Throwable ex) {
        return false;
      }
    }).start();
  }

  protected void onAppStartup() {
    if (stereoHttpClient.canStart()) {
        stereoHttpClient.start();
    }
  }
  protected <T> CompletableFuture<T> onAppShutdown() {
    executorService.shutdownNow();
    return CompletableFuture.completedFuture(null);
  }

  public static class Cfg {
    public boolean isProd = false;
  }
}


```
## Zool Service Mesh
Zool Service Mesh is a wrapper around Zool which allows a container application to become aware of the zookeeper network by announcing itself, and also getting a copy of the other hosts that have already announced. Each zookeeperHost connected to the same Zookeeper quarum gets a copy of the entire map.  

The ServiceHub can be Injected with JavaX or Guice injection and requires a Zool instance, and an ExecutorService
```java
bind(ZoolServiceMesh.class).asEagerSingleton();
```
Then you can set it up like so:
```java
@Inject
public YourClass(ZoolServiceMesh zoolServiceMesh) {
...
}

// zk port
zoolServiceMesh.setPort(2181);

// the service key that this zookeeperHost will live in
zoolServiceMesh.setServiceKey("userService");
// if production true, false for dev
zoolServiceMesh.setProd(true);
// this will start the hub, and announce our local url, as well as exchange for a copy of the other
// services on the network, if there are any. This requires Zookeeper to be running at the configured ip / port
zoolServiceMesh.start();
```

You can later stop the hub:
```java
// stops the service hub (unannouncing effectively your services zookeeperHost)
zoolServiceMesh.stop();
```
To get known services based on the gateway service map and service keys:

```java
  List<String> knownServiceKeys = zoolServiceMesh.getKnownServices();
  List<String> hostsForUserService = zoolServiceMesh.getHostsForService("userService");
  
  // keys will be something similar to { "userService" }
  // hosts for userService would be something such as { "localhost:9001", "localhost:9002" }
```
### Service and Gateway Nodes
The ZoolServiceMesh will automatically create the gateway and service map nodes based on the
configuration.  Example:
```
serviceMap: {
  authorization: {
    ec2-3-15-204-21.us-east-2.compute.amazonaws.com:9000: {
      token: "...",
      currentEpochTime: 1570115378945,
      securehost: false
    }
  },
  identity: {
    ec2-3-15-13-254.us-east-2.compute.amazonaws.com:9000: {
      token: "...",
      currentEpochTime: 1570115339645,
      securehost: false
    }
  },
  flagship: {
    ec2-18-188-238-37.us-east-2.compute.amazonaws.com:9000: {
     token: "...",
     currentEpochTime: 1570313803558,
     securehost: false
    },
    ec2-18-191-91-113.us-east-2.compute.amazonaws.com:9000: {
      token: "...",
      currentEpochTime: 1570313786184,
      securehost: false
    }
  },
  dynamicdiscovery: {
    ec2-3-19-237-167.us-east-2.compute.amazonaws.com:9000: {
      token: "...",
      currentEpochTime: 1570115200558,
      securehost: false
    }
  },
  example: {
    ec2-3-15-200-96.us-east-2.compute.amazonaws.com:9000: {
      token: "...",
      currentEpochTime: 1570115339653,
      securehost: false
    }
  }
}

```
To overlap, you might want to share the login service above between the userService and the adminService in some way.

Both user service and admin service provide an additional ZoolServiceHub instance, pointing to "serviceMap0" as its gateway.

`NOTE:` This particular system is still under consideration!

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

