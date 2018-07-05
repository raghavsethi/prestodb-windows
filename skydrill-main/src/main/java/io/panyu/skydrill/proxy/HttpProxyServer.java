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

import io.airlift.event.client.NullEventClient;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.RequestStats;
import io.airlift.node.NodeInfo;
import io.airlift.tracetoken.TraceTokenManager;
import org.eclipse.jetty.proxy.AsyncMiddleManServlet;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static io.panyu.skydrill.server.SkydrillConfig.getCoordinatorServiceUri;

public class HttpProxyServer
        extends io.airlift.http.server.HttpServer
{
  private static Map<String, String> parameters = new HashMap<String, String>() {{ put("preserveHost", "true"); }};

  @Inject
  public HttpProxyServer(ProxyServerConfig config, NodeInfo nodeInfo) throws Exception {
    super(
            new HttpServerInfo(getHttpServerConfig(config), nodeInfo),
            nodeInfo,
            getHttpServerConfig(config),
            // reverse proxy servlet
            new ServletHolder(new AsyncMiddleManServlet() {
              private String getProxyTo() {
                Optional<String> proxyTo = Optional.ofNullable(getCoordinatorServiceUri());
                return proxyTo.orElse(String.format("http://0.0.0.0:%s", config.getHttpPort()));
              }

              @Override
              protected String rewriteTarget(HttpServletRequest clientRequest) {
                StringBuilder builder = new StringBuilder();
                builder.append(getProxyTo());
                builder.append(clientRequest.getRequestURI());
                String query = clientRequest.getQueryString();
                if (query != null)
                  builder.append("?").append(query);
                return builder.toString();
              }
            }).getServlet(),
            ImmutableMap.copyOf(parameters),
            ImmutableSet.of(),
            ImmutableSet.of(),
            null,
            null,
            ImmutableSet.of(),
            null,
            null,
            new TraceTokenManager(),
            new RequestStats(),
            new NullEventClient()
    );
  }

  private static HttpServerConfig getHttpServerConfig(ProxyServerConfig config) {
    config.setHttpEnabled(false);
    config.setHttpsEnabled(false);

    if (config.isEnabled()) {
      if (config.isSslEnabled()) {
        config.setHttpsEnabled(true);
        config.setHttpsPort(config.getProxyPort());
      } else {
        config.setHttpEnabled(true);
        config.setHttpPort(config.getProxyPort());
      }
    }
    return config;
  }
}