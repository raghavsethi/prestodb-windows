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
package io.panyu.skydrill.proxy;

import io.airlift.configuration.Config;

public class ProxyServerConfig
        extends io.airlift.http.server.HttpServerConfig
{
  private boolean enabled = true;
  private boolean sslEnabled = false;
  private int port = 443;

  public boolean isEnabled() {
    return enabled;
  }

  @Config("proxy-server.enabled")
  public ProxyServerConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  @Config("proxy-server.ssl-enabled")
  public ProxyServerConfig setSslEnabled(boolean enabled) {
    this.sslEnabled = enabled;
    return this;
  }

  public int getProxyPort() {
    return port;
  }

  @Config("proxy-server.port")
  public ProxyServerConfig setProxyPort(int port) {
    this.port = port;
    return this;
  }

}