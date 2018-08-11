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

import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.GracefulShutdownModule;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.event.client.HttpEventModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.panyu.skydrill.discovery.DiscoveryModule;
import io.panyu.skydrill.election.LeaderElection;
import io.panyu.skydrill.election.LeaderElectionWatcher;
import io.panyu.skydrill.metadata.CatalogStore;
import io.panyu.skydrill.operator.aggregation.DefaultHyperLogLogAggregation;
import io.panyu.skydrill.operator.aggregation.HyperLogLogAggregation;
import io.panyu.skydrill.operator.aggregation.MergeQuantileDigestAggregation;
import io.panyu.skydrill.operator.aggregation.QuantileDigestAggregation;
import io.panyu.skydrill.operator.scalar.QuantileDigestFunctions;
import io.panyu.skydrill.proxy.HttpProxyServer;
import org.weakref.jmx.guice.MBeanModule;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;

public class SkydrillServer implements Runnable {

    public static void main(String[] args) {
        if (args.length > 0) {
            if (args[0].startsWith("--help")) {
                System.out.println("Usage: " + SkydrillServer.class.getSimpleName() + " config.properties");
                return;
            }
            System.setProperty("config", args[0]);
        }

        new SkydrillServer().run();
    }

    private final SqlParserOptions sqlParserOptions;

    public SkydrillServer() {
        this(new SqlParserOptions());
    }

    public SkydrillServer(SqlParserOptions sqlParserOptions) {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    public void run()
    {
        Logger log = Logger.get(PrestoServer.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new SkydrillModule(),
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new MBeanModule(),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new HttpEventModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new EventListenerModule(),
                new ServerMainModule(sqlParserOptions),
                new GracefulShutdownModule());

        modules.addAll(getAdditionalModules());
        Bootstrap app = new Bootstrap(modules.build());

        List<SqlFunction> functions = new FunctionListBuilder()
                .aggregate(HyperLogLogAggregation.class)
                .aggregate(DefaultHyperLogLogAggregation.class)
                .aggregate(MergeQuantileDigestAggregation.class)
                .aggregates(QuantileDigestAggregation.class)
                .scalars(QuantileDigestFunctions.class)
                .getFunctions();

        try {
            Injector injector = app.strictConfig().initialize();
            injector.getInstance(PluginManager.class).loadPlugins();
            injector.getInstance(MetadataManager.class).addFunctions(functions);
            injector.getInstance(StaticCatalogStore.class).loadCatalogs();
            injector.getInstance(CatalogStore.class).loadCatalogs();

            updateConnectorIds(
                    injector.getInstance(Announcer.class),
                    injector.getInstance(CatalogManager.class),
                    injector.getInstance(ServerConfig.class),
                    injector.getInstance(NodeSchedulerConfig.class));

            injector.getInstance(SessionSupplier.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(EventListenerManager.class).loadConfiguredEventListener();

            injector.getInstance(Announcer.class).start();
            startCoordinatorHA(injector);
            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    private static void startCoordinatorHA(Injector injector) throws Exception {
        ServerConfig serverConfig = injector.getInstance(ServerConfig.class);
        if (serverConfig.isCoordinator()) {
            injector.getInstance(LeaderElection.class).start();
            injector.getInstance(HttpProxyServer.class).start();
        } else {
            injector.getInstance(LeaderElectionWatcher.class).start();
        }
    }

    private Iterable<? extends Module> getAdditionalModules() {
        return ImmutableList.of();
    }

    private static void updateConnectorIds(Announcer announcer,
                                           CatalogManager metadata,
                                           ServerConfig serverConfig,
                                           NodeSchedulerConfig schedulerConfig) {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // get existing connectorIds
        String property = nullToEmpty(announcement.getProperties().get("connectorIds"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);

        // automatically build connectorIds if not configured
        if (connectorIds.isEmpty()) {
            List<Catalog> catalogs = metadata.getCatalogs();
            // if this is a dedicated coordinator, only add jmx
            if (serverConfig.isCoordinator() && !schedulerConfig.isIncludeCoordinator()) {
                catalogs.stream()
                        .map(Catalog::getConnectorId)
                        .filter(connectorId -> connectorId.getCatalogName().equals("jmx"))
                        .map(Object::toString)
                        .forEach(connectorIds::add);
            }
            else {
                catalogs.stream()
                        .map(Catalog::getConnectorId)
                        .map(Object::toString)
                        .forEach(connectorIds::add);
            }
        }

        // build announcement with updated sources
        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements) {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }
}
