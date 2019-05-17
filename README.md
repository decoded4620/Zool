# Zool
Zool is an interface for synchronizing microservice configuration on Apache Zookeeper.

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


### Creating your service and gateway nodes
This assumes you have Zookeeper installed on your Linux, Mac, or Windows system.
1. Run your Zookeeper Server (e.g. localhost:2181)
2. Open zkCli.sh (or zkCli.bat for windows)
Once you see

```[zk: localhost:2181(CONNECTED) 0]```

You can create your `services` and `gateway` nodes.

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

