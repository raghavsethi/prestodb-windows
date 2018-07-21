## **Coordinator High Availability (HA)**

Skydrill Coordinator HA enables uninterrupted query serving in case of hosting machine failure. Coordinator HA is implemented using leader election and access proxy, together with the Presto discovery client extended to reroute when lead coordinator changes.

Zookeeper is used to provide reliable distributed coordination to perform leader election. Zookeeper is highly concurrent, optimized for read-heavy access, and very fast. The coordinator proxy is used to facilate access to the lead coordinator, and it can serve as access security provider as well. Coordinator proxy is built into the Skydrill coordinator.

### Configuration

For development or single-box deployment, embedded instance of Zookeeper and proxy service are turned on by default so there is no need for additional setup. For production deployment, it is recommendted to setup 2*N+1 number of coordinator nodes.

Use below key value pairs to configure access to extenal Zookeeper service, replacing `zooX:2181` with the actual endpoints:

    zookeeper.server-enabled=false
    zookeeper.connect-string=zoo1:2181,zoo2:2181,zoo3:2181

Proxy service is configured using these keys:

    proxy-server.enabled
    proxy-server.port

To enable SSL, authorization, and authentication use these keys below:

    proxy-server.ssl-enabled
    http-server.https.keystore.path    
    http-server.https.keystore.key
    http-server.https.keymanager.password
    http-server.https.truststore.path
    http-server.https.truststore.key

Fine grain access tuning to Zookeeper can be done via these configuration properties:

    zookeeper.session-timeout
    zookeeper.connection-timeout
    curator.initialize-wait-time
    curator.retry-sleep-time
    curator.retry-max-retries

