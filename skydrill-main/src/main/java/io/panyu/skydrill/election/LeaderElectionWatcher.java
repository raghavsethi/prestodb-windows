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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.panyu.skydrill.server.SkydrillConfig.setCoordinatorServiceUri;
import static io.panyu.skydrill.server.SkydrillConfig.setDiscoveryServiceUri;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

public class LeaderElectionWatcher
        implements CuratorWatcher
{
  private static final Logger log = Logger.get(LeaderElectionWatcher.class);

  private final LeaderElectionConfig config;
  private final CuratorFramework curator;
  private final String leaderElectionPath;
  
  private URI coordinatorUri;

  @Inject
  public LeaderElectionWatcher(LeaderElectionConfig config, CuratorFramework curator) {
    this.config = requireNonNull(config, "config is null");
    this.curator = requireNonNull(curator, "curator is null");
    this.leaderElectionPath = requireNonNull(config.getLeaderElectionPath());

    try {
      coordinatorUri = requireNonNull(config.getLocalCoordinatorURI());
    } catch (Exception e) {
      log.error(e);
    }
  }

  public void start() {
    try {
      if (curator.checkExists().forPath(leaderElectionPath) == null) {
        curator.create().creatingParentContainersIfNeeded().forPath(leaderElectionPath);
      }
      curator.getData().usingWatcher(this).forPath(leaderElectionPath);
    } catch (Exception e) {
      log.error(e);
    }

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, daemonThreadsNamed("coordinator-watcher"));
    scheduler.scheduleWithFixedDelay(this::refreshCoordinatorUri, 0, config.getPullingInterval(), TimeUnit.SECONDS);
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));
  }

  public URI getCoordinatorURI() {
    return coordinatorUri;
  }

  @Override
  public void process(WatchedEvent event) throws Exception {
    Watcher.Event.EventType eventType = event.getType();
    switch (eventType){
      case NodeDataChanged:
      case NodeChildrenChanged:
        refreshCoordinatorUri();
        break;
      default:
        break;
    }
    curator.getData().usingWatcher(this).forPath(leaderElectionPath);
  }

  private void refreshCoordinatorUri() {
    try {
      String coordinatorUriString = new String(curator.getData().forPath(config.getLeaderElectionPath()), Charsets.UTF_8);
      setCoordinatorServiceUri(coordinatorUriString);
      setDiscoveryServiceUri(coordinatorUriString);
      coordinatorUri = new URI(coordinatorUriString);
      log.info("lead coordinator is: " + coordinatorUri);
    } catch (Exception e) {
      log.error(e);
    }
  }
}
