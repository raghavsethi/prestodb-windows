package io.panyu.skydrill.metadata;

import com.facebook.presto.connector.ConnectorManager;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static com.google.common.collect.Maps.fromProperties;

public class SkydrillCatalogStore
        implements CuratorWatcher
{
    private static final Logger log = Logger.get(SkydrillCatalogStore.class);

    private final ConnectorManager connectorManager;
    private final SkydrillCatalogStoreConfig config;
    private final CuratorFramework curator;
    private final String catalogRootPath;

    @Inject
    public SkydrillCatalogStore(ConnectorManager connectorManager,
                                SkydrillCatalogStoreConfig config,
                                CuratorFramework curator) {
        this.connectorManager = requireNonNull(connectorManager);
        this.config = requireNonNull(config);
        this.curator = requireNonNull(curator);
        this.catalogRootPath = requireNonNull(config.getCatalogRootPath());

        try {
            if (curator.checkExists().forPath(catalogRootPath) == null) {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(catalogRootPath);
            }
            curator.getData().usingWatcher(this).forPath(catalogRootPath);
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void loadCatalogs() {
        refreshCatalogs();
    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        Watcher.Event.EventType eventType = event.getType();
        switch (eventType){
            case NodeDataChanged:
            case NodeChildrenChanged:
                refreshCatalogs();
                break;
            default:
                break;
        }
        curator.getData().usingWatcher(this).forPath(catalogRootPath);
    }

    private synchronized void refreshCatalogs() {
        try {
            curator.getChildren().forPath(catalogRootPath)
                    .forEach(this::loadCatalog);
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void loadCatalog(String catalogName) {
        try {
            Properties properties = new Properties();
            String data = new String(curator.getData().forPath(String.format("%s/%s", catalogRootPath, catalogName)));
            properties.load(new StringReader(data));

            log.info("-- Loading catalog %s --", catalogName);
            Map<String, String> map = new HashMap<>(fromProperties(properties));
            String connectorName = map.remove("connector.name");
            connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(map));
            log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
        } catch (Exception e) {
            log.error(e);
        }
    }
}
