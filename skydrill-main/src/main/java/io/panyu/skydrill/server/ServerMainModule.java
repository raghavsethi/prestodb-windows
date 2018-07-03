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

import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.panyu.skydrill.election.LeaderElection;
import io.panyu.skydrill.election.LeaderElectionConfig;
import io.panyu.skydrill.election.LeaderElectionWatcher;
import io.panyu.skydrill.proxy.HttpProxyServer;
import io.panyu.skydrill.proxy.ProxyServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class ServerMainModule
        extends com.facebook.presto.server.ServerMainModule
{
    public ServerMainModule(SqlParserOptions sqlParserOptions) {
        super(sqlParserOptions);
    }

    @Override
    protected void setup(Binder binder) {
      super.setup(binder);

      jaxrsBinder(binder).bind(SkydrillResource.class);
      configBinder(binder).bindConfig(LeaderElectionConfig.class);

      ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
      if (serverConfig.isCoordinator()) {
        httpServerBinder(binder).bindResource("/cat", "skydrill").withWelcomeFile("cat.html");

        binder.bind(LeaderElection.class).in(Scopes.SINGLETON);
        binder.bind(HttpProxyServer.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ProxyServerConfig.class);
      } else {
        binder.bind(LeaderElectionWatcher.class).in(Scopes.SINGLETON);
      }

    }
}