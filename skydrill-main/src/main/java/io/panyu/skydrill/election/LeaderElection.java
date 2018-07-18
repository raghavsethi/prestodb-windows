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
package io.panyu.skydrill.election;

import com.google.common.base.Charsets;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.panyu.skydrill.server.SkydrillConfig.setCoordinatorLeadership;
import static io.panyu.skydrill.server.SkydrillConfig.setCoordinatorServiceUri;
import static io.panyu.skydrill.server.SkydrillConfig.setDiscoveryServiceUri;
import static java.util.Objects.requireNonNull;

public class LeaderElection
        implements LeaderSelectorListener, CuratorWatcher
{
  private static final Logger log = Logger.get(LeaderElection.class);
  private static final int electionCountDownSecond = 2;

  private final CountDownLatch exitCountDownLatch = new CountDownLatch(1);
  private final CountDownLatch electionCountDown = new CountDownLatch(1);
  private final LeaderElectionConfig config;
  private final CuratorFramework curator;
  private final LeaderSelector leaderSelector;

  @Inject
  public LeaderElection(LeaderElectionConfig config,
                        CuratorFramework curator) {
    this.config = requireNonNull(config);
    this.curator = requireNonNull(curator);

    leaderSelector = new LeaderSelector(curator, config.getLeaderElectionPath(), this);
    try {
      URI thisCoordinator = config.getLocalCoordinatorURI();
      String leaderId = String.format("%s:%s", thisCoordinator.getHost(), thisCoordinator.getPort());
      prepareLeaderElectionPath(leaderId);
      
      leaderSelector.setId(leaderId);
    } catch (Exception e) {
      log.error(e);
    }
  }

  public void start() throws Exception {
    leaderSelector.start();
    electionCountDown.await(electionCountDownSecond, TimeUnit.SECONDS);

    if (!leaderSelector.hasLeadership()) {
      setCoordinatorLeadership(false);
      updateLeaderCoordinatorURI();
      curator.getData().usingWatcher(this).forPath(config.getLeaderElectionPath());
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutting down...");
      leaderSelector.close();
    }));
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    log.info("takeLeadership");

    String localCoordinatorURI = config.getLocalCoordinatorURI().toString();
    curator.setData().forPath(config.getLeaderElectionPath(), localCoordinatorURI.getBytes(Charsets.UTF_8));
    setCoordinatorServiceUri(localCoordinatorURI);
    setDiscoveryServiceUri(localCoordinatorURI);
    setCoordinatorLeadership(true);
    log.info(String.format("lead coordinator is %s", localCoordinatorURI));

    electionCountDown.countDown();
    exitCountDownLatch.await();
    log.info("relinquished leadership");
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    switch (newState) {
      case LOST:
        log.error("zookeeper connection lost");
        executeFailFast();
        break;
      case SUSPENDED:
        log.warn("zookeeper connection suspended");
        executeFailFast();
        break;
      default:
        log.info("zookeeper connection state changed: " + newState);
    }
  }

  @Override
  public void process(WatchedEvent event) throws Exception {
    curator.getData().usingWatcher(this).forPath(config.getLeaderElectionPath());
    if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
      updateLeaderCoordinatorURI();
    }
  }

  private void prepareLeaderElectionPath(String leaderId) {
    String leaderElectionPath = config.getLeaderElectionPath();
    try {
      if (curator.checkExists().forPath(leaderElectionPath) == null) {
        curator.create().creatingParentContainersIfNeeded().forPath(leaderElectionPath);
      }

      for (String path : curator.getChildren().forPath(leaderElectionPath)) {
        String fullPath = String.format("%s/%s", leaderElectionPath, path);
        String data = new String(curator.getData().forPath(fullPath), Charsets.UTF_8);
        if (data.equals(leaderId)) {
          curator.delete().forPath(fullPath);
        }
      }
    } catch (Exception e) {
      log.error(e);
    }
  }

  private void updateLeaderCoordinatorURI() throws Exception {
    Optional<byte[]> bytes = Optional.ofNullable(curator.getData().forPath(config.getLeaderElectionPath()));
    bytes.ifPresent(x -> {
      String leadCoordinatorUri = new String(x, Charsets.UTF_8);
      setCoordinatorServiceUri(leadCoordinatorUri);
      log.info(String.format("lead coordinator is %s", leadCoordinatorUri));
    });
  }

  private void executeFailFast() {
    Runtime.getRuntime().exit(1);
  }
}