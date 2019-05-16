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
