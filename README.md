# Zool

<img src="./docs/images/zuul.jpg" width="33%">

Zool is an interface for synchronizing microservice configuration on Apache Zookeeper.

## JavaDocs
Read them [Here](https://decoded4620.github.io/Zool/docs/javadoc/)

## How to Zool
### Implement a Zool Client
You can Implement your own Zool Client simply by extending the Zool Abstract

```java
package your.cool.app.ZoolClient;

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
Zool Service Hub is a wrapper around Zool which allows a container application to become aware of the zookeeper network by announcing itself, and also getting a copy of the other hosts that have already announced. Each host connected to the same Zookeeper quarum gets a copy of the entire map.  

The ServiceHub can be Injected with JavaX or Guice injection and requires a Zool instance, and an ExecutorService
```java
// ZoolServiceHub binding in your Container Module
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

// the service key that this host will live in
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
// stops the service hub (unannouncing effectively your services host)
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

