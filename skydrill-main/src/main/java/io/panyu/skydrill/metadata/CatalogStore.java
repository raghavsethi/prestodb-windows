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
package io.panyu.skydrill.metadata;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;
import static com.google.common.collect.Maps.fromProperties;
import static io.panyu.skydrill.server.SkydrillConfig.hasCoordinatorLeadership;

public class CatalogStore
        implements CuratorWatcher
{
    private static final Logger log = Logger.get(CatalogStore.class);

    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final Announcer announcer;
    private final CatalogStoreConfig config;
    private final CuratorFramework curator;
    private final String catalogRootPath;

    @Inject
    public CatalogStore(ConnectorManager connectorManager,
                        CatalogManager catalogManager,
                        Announcer announcer,
                        CatalogStoreConfig config,
                        CuratorFramework curator) {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
        this.config = requireNonNull(config, "CatalogStoreConfig is null");
        this.curator = requireNonNull(curator, "curator is null");
        this.catalogRootPath = requireNonNull(config.getCatalogRootPath(), "catalog root is null");

        try {
            if (curator.checkExists().forPath(catalogRootPath) == null) {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(catalogRootPath);
            }
            curator.getData().usingWatcher(this).forPath(catalogRootPath);
        } catch (Exception e) {
            log.error(e);
        }
    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        Optional<byte[]> bytes = Optional.ofNullable(curator.getData().usingWatcher(this).forPath(catalogRootPath));
        boolean forSubscribers = !config.isCoordinator() || !hasCoordinatorLeadership();
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            if (forSubscribers) {
                bytes.ifPresent(x -> {
                    String nodeData = new String(x);
                    String catalogName = nodeData.substring(1);
                    if (nodeData.startsWith("+")) {
                        try {
                            loadCatalog(catalogName);
                            announceUpdate(catalogName);
                        } catch (Exception e) {
                            log.error(e);
                        }
                    }
                    if (nodeData.startsWith("-")) {
                        dropCatalog(catalogName);
                    }
                });
            }
        }
    }

    public synchronized void loadCatalogs() throws Exception {
        curator.getChildren().forPath(catalogRootPath).forEach(this::loadCatalog);
    }

    public synchronized void loadCatalog(String catalogName) {
        loadCatalog(catalogName, false);
    }

    public synchronized void loadCatalog(String catalogName, boolean forPublishing) {
        try {
            String catalogPath = String.format("%s/%s", catalogRootPath, catalogName);
            Optional<byte[]> bytes = Optional.ofNullable(curator.getData().forPath(catalogPath));
            if (bytes.isPresent()) {
                Properties properties = new Properties();
                properties.load(new ByteArrayInputStream(bytes.get()));

                log.info("-- Loading catalog %s --", catalogName);
                Map<String, String> map = new HashMap<>(fromProperties(properties));
                String connectorName = map.remove("connector.name");
                connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(map));
                log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);

                if (forPublishing) {
                    curator.setData().forPath(catalogRootPath, ("+" + catalogName).getBytes());
                    announceUpdate(catalogName);
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    public synchronized void dropCatalog(String catalogName) {
        dropCatalog(catalogName, false);
    }

    public synchronized void dropCatalog(String catalogName, boolean forPublishing) {
        try {
            log.info("-- Unloading catalog %s --", catalogName);
            connectorManager.dropConnection(catalogName);
            log.info("-- Removed catalog %s --", catalogName);

            if (forPublishing) {
                curator.setData().forPath(catalogRootPath, ("-" + catalogName).getBytes());
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    public List<String> getCatalogs() {
        return catalogManager.getCatalogs().stream().map(Catalog::getCatalogName)
                .collect(Collectors.toList());
    }

    private void announceUpdate(String connectorId) throws Exception {
        ServiceAnnouncement announcement = announcer.getServiceAnnouncements().stream()
                .filter(x -> x.getType().equals("presto"))
                .collect(onlyElement());
        String connectorIds = announcement.getProperties().get("connectorIds");

        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", String.format("%s,%s", connectorIds, connectorId));

        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
        announcer.forceAnnounce().get(30, TimeUnit.SECONDS);
    }
}
