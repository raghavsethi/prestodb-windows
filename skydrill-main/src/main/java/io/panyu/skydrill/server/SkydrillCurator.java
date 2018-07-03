/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.panyu.skydrill.server;

import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import io.airlift.log.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.util.concurrent.TimeUnit;

public class SkydrillCurator
        extends CuratorFrameworkImpl
        implements ConnectionStateListener
{
  private static final Logger log = Logger.get(SkydrillCurator.class);

  @Inject
  public SkydrillCurator(SkydrillConfig config) throws Exception {
    super(CuratorFrameworkFactory.builder()
            .connectString(config.getConnectString())
            .sessionTimeoutMs(config.getSessionTimeoutMs())
            .connectionTimeoutMs(config.getConnectionTimeoutMs())
            .retryPolicy(new ExponentialBackoffRetry(config.getBaseSleepTime(), config.getMaxRetries()))
    );

    System.setProperty("zookeeper.connect.string", config.getConnectString());
    if (config.isZookeeperEnabled() && config.isCoordinator()) {
      log.info("start embedded zookeeper server - for development only");
      new Thread(new EmbeddedZookeeper(config.getZookeeperConfigFile(), new QuorumPeerConfig())).start();
    }

    this.start();
    this.blockUntilConnected(10, TimeUnit.SECONDS);
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    if (newState == ConnectionState.LOST) {
      log.info("Connection lost, halting");
      Runtime.getRuntime().halt(1);
    }
  }

  private class EmbeddedZookeeper
          implements Runnable {
    private final String zookeeperConfig;
    private final QuorumPeerConfig quorumPeerConfig;

    private EmbeddedZookeeper(String zookeeperConfig, QuorumPeerConfig quorumPeerConfig) {
      this.zookeeperConfig = zookeeperConfig;
      this.quorumPeerConfig = quorumPeerConfig;
    }

    @Override
    public void run() {
      try {
        ZooKeeperServerMain server = new ZooKeeperServerMain();
        ServerConfig config = new ServerConfig();
        quorumPeerConfig.parse(zookeeperConfig);
        config.readFrom(quorumPeerConfig);

        server.runFromConfig(config);
      } catch (Throwable e) {
        throw new SkydrillException(e);
      }
    }
  }
}
