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

import io.airlift.configuration.Config;

import java.util.Optional;

public class SkydrillConfig {
  private static final String CoordinatorUriKey = "coordinator.uri";
  private static final String CoordinatorLeaderKey = "coordinator.leader";
  private static final String DiscoveryUriKey = "discovery.uri";

  private boolean zookeeperEnabled = true;
  private String zookeeperConfig = "etc/zookeeper.config";
  private String connectString = "localhost:2181";
  private int sessionTimeoutMs = 60 * 1000;
  private int connectionTimeoutMs = 15 * 1000;
  
  private int baseSleepTime = 1000;
  private int maxRetries = 5;
  private int maxWaitTime = 10;

  private boolean coordinator = true;

  public static String getCoordinatorServiceUri() {
    return System.getProperty(CoordinatorUriKey);
  }

  public static void setCoordinatorServiceUri(String uri) {
    System.setProperty(CoordinatorUriKey, uri);
  }

  public static boolean hasCoordinatorLeadership() {
    return Optional.ofNullable(System.getProperty(CoordinatorLeaderKey))
            .map(x -> x.equals(String.valueOf(true))).orElse(false);
  }

  public static void setCoordinatorLeadership(boolean hasLeadership) {
    System.setProperty(CoordinatorLeaderKey, String.valueOf(hasLeadership));
  }

  public static String getDiscoveryServiceUri() {
    return System.getProperty(DiscoveryUriKey);
  }

  public static void setDiscoveryServiceUri(String uri) {
    System.setProperty(DiscoveryUriKey, uri);
  }

  public boolean isCoordinator() {
    return coordinator;
  }

  @Config("coordinator")
  public SkydrillConfig setCoordinator(boolean coordinator) {
    this.coordinator = coordinator;
    return this;
  }

  public boolean isZookeeperEnabled() {
    return zookeeperEnabled;
  }

  @Config("zookeeper.server-enabled")
  public SkydrillConfig setZookeeperEnabled(boolean embedZookeeper) {
    this.zookeeperEnabled = embedZookeeper;
    return this;
  }

  public String getZookeeperConfigFile() {
    return zookeeperConfig;
  }

  @Config("zookeeper.config-file")
  public SkydrillConfig setZookeeperConfigFile(String zookeeperConfig) {
    this.zookeeperConfig = zookeeperConfig;
    return this;
  }

  public String getConnectString() {
    return connectString;
  }

  @Config("zookeeper.connect-string")
  public SkydrillConfig setConnectString(String connectString) {
    this.connectString = connectString;
    return this;
  }

  public int getSessionTimeoutMs() {
    return sessionTimeoutMs;
  }

  @Config("zookeeper.session-timeout")
  public SkydrillConfig setSessionTimeoutMs(int sessionTimeout){
    this.sessionTimeoutMs = sessionTimeout;
    return this;
  }

  public int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  @Config("zookeeper.connection-timeout")
  public SkydrillConfig setConnectionTimeoutMs(int connectionTimeout) {
    this.connectionTimeoutMs = connectionTimeout;
    return this;
  }
  
  public int getBaseSleepTime() {
    return baseSleepTime;
  }

  @Config("curator.retry-sleeptime")
  public SkydrillConfig setBaseSleepTime(int baseSleepTime) {
    this.baseSleepTime = baseSleepTime;
    return this;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  @Config("curator.retry-maxretries")
  public SkydrillConfig setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public int getMaxWaitTime() {
    return maxWaitTime;
  }

  @Config("curator.initialize-waittime")
  public SkydrillConfig setMaxWaitTime(int maxWaitTime) {
    this.maxWaitTime = maxWaitTime;
    return this;
  }
}
