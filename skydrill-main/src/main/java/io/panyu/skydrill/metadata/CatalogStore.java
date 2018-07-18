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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static com.google.common.collect.Maps.fromProperties;

public class CatalogStore
        implements CuratorWatcher
{
    private static final Logger log = Logger.get(CatalogStore.class);

    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final CatalogStoreConfig config;
    private final CuratorFramework curator;
    private final String catalogRootPath;

    @Inject
    public CatalogStore(ConnectorManager connectorManager,
                        CatalogManager catalogManager,
                        CatalogStoreConfig config,
                        CuratorFramework curator) {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
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
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            if (!config.isCoordinator()) {
                bytes.ifPresent(x -> {
                    String nodeData = new String(x);
                    if (nodeData.startsWith("+")) {
                        loadCatalog(nodeData.substring(1));
                    }
                    if (nodeData.startsWith("-")) {
                        dropCatalog(nodeData.substring(1));
                    }
                });
            }
        }
    }

    public synchronized void loadCatalogs() throws Exception {
        curator.getChildren().forPath(catalogRootPath).forEach(this::loadCatalog);
    }

    public synchronized void loadCatalog(String catalogName) {
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

                if (config.isCoordinator()) {
                    curator.setData().forPath(catalogRootPath, ("+" + catalogName).getBytes());
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    public synchronized void dropCatalog(String catalogName) {
        try {
            log.info("-- Unloading catalog %s --", catalogName);
            connectorManager.dropConnection(catalogName);
            log.info("-- Removed catalog %s --", catalogName);

            if (config.isCoordinator()) {
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
}
