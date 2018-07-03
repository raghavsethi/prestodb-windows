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

import io.airlift.configuration.Config;
import io.airlift.http.server.HttpServerConfig;

import java.net.InetAddress;
import java.net.URI;

public class LeaderElectionConfig
        extends HttpServerConfig
{
  private String leaderElectionPath = "/skydrill/runtime/coordinator";
  private String workerRuntimPath = "/skydrill/runtime/worker";
  private int pullingInterval = 60;

  public String getLeaderElectionPath() {
    return leaderElectionPath;
  }

  @Config("leader.election.coordinator.path")
  public LeaderElectionConfig setLeaderElectionPath(String leaderElectionPath) {
    this.leaderElectionPath = leaderElectionPath;
    return this;
  }

  public String getWorkerRuntimePath() {
    return workerRuntimPath;
  }

  @Config("worker.runtime.path")
  public LeaderElectionConfig setWorkerRuntimePath(String workerRuntimePath) {
    this.workerRuntimPath = workerRuntimePath;
    return this;
  }

  public int getPullingInterval() {
    return pullingInterval;
  }

  @Config("leader.election.pulling.interval")
  public LeaderElectionConfig setPullingInterval(int pullingInterval) {
    this.pullingInterval = pullingInterval;
    return this;
  }

  protected URI getLocalCoordinatorURI() throws Exception {
    return URI.create(String.format("%s://%s:%s",
            isHttpsEnabled()? "https" : "http",
            InetAddress.getLocalHost().getHostAddress(),
            isHttpsEnabled()? getHttpsPort() : getHttpPort()));
  }

}