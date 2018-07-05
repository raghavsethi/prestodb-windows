package io.panyu.skydrill.plugin.jdbc.metastore;

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ZkJdbcMetastore
        implements JdbcMetastore, Closeable
{
  private static final Logger log = Logger.get(ZkJdbcMetastore.class);
  private static final Splitter nsSplitter = Splitter.on('.').trimResults().omitEmptyStrings();
  private static final JsonCodecFactory jsonCodecFactory = new JsonCodecFactory();

  private final JdbcConnectorId connectorId;
  private final String viewRootPath;
  private final JsonCodec<ViewDefinition> viewCodec;
  private final CuratorFramework curator;

  @Inject
  public ZkJdbcMetastore(JdbcConnectorId connectorId,
                         ZkJdbcMetastoreConfig config) {
    this.connectorId = connectorId;
    this.viewRootPath = String.format("%s/%s", config.getViewRootPath(), connectorId);
    this.viewCodec = jsonCodecFactory.jsonCodec(ViewDefinition.class);

    RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);
    curator = CuratorFrameworkFactory.newClient(config.getConnectString(), retry);
  }

  public CuratorFramework getCurator() throws Exception {
    checkAndStartCurator();
    return curator;
  }

  @Override
  public void createView(SchemaTableName viewName, String viewData, boolean replace) {
    try {
      checkAndStartCurator();
      String viewPath = getPathString(viewRootPath, viewName);
      if (curator.checkExists().forPath(viewPath) == null) {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(viewPath, viewData.getBytes());
      } else if (replace) {
        curator.setData().forPath(viewPath, viewData.getBytes());
      }
    } catch (Throwable e) {
      log.error(e);
    }
  }

  @Override
  public void dropView(SchemaTableName viewName) {
    try {
      checkAndStartCurator();
      String viewPath = getPathString(viewRootPath, viewName);
      if (curator.checkExists().forPath(viewPath) != null) {
        curator.delete().forPath(viewPath);
      }
    } catch (Throwable e){
      log.error(e);
    }
  }

  @Override
  public List<SchemaTableName> listViews(String schema) {
    try {
      checkAndStartCurator();
      return curator.getChildren().forPath(viewRootPath)
              .stream()
              .filter(x -> x.startsWith(schema))
              .map(this::getSchemaTableFromPath)
              .collect(toImmutableList());
    } catch (Throwable e) {
      log.error(e);
    }
    return ImmutableList.of();
  }

  @Override
  public Map<SchemaTableName, ConnectorViewDefinition> getViews(SchemaTablePrefix prefix) {
    return ImmutableMap.of();
  }

  @Override
  public String getViewData(SchemaTableName viewName) {
    try {
      checkAndStartCurator();
      String viewPath = getPathString(viewRootPath, viewName);
      Stat stat = curator.checkExists().forPath(viewPath);
      if ((stat != null) && (stat.getDataLength() > 0)) {
        return new String(curator.getData().forPath(viewPath));
      }
    } catch (Throwable e){
      log.error(e);
    }
    return null;
  }

  @Override
  public ViewDefinition getViewDefinition(SchemaTableName viewName) {
    try {
      String viewData = getViewData(viewName);
      if (viewData != null) {
        return viewCodec.fromJson(viewData);
      }
    } catch (Exception e) {
      log.error(e);
    }
    return null;
  }

  public boolean isView(SchemaTableName viewName){
    try{
      String viewPath = getPathString(viewRootPath, viewName);
      return curator.checkExists().forPath(viewPath) != null;
    } catch (Throwable e){
      return false;
    }
  }

  private void checkAndStartCurator() throws Exception {
    if (curator.getState() != CuratorFrameworkState.STARTED) {
      curator.start();
      curator.blockUntilConnected(10, TimeUnit.SECONDS);

      if (curator.checkExists().forPath(viewRootPath) == null) {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(viewRootPath);
      }
    }
  }

  private String getPathString(String rootPath, SchemaTableName table) {
    return String.format("%s/%s.%s", rootPath, table.getSchemaName(), table.getTableName());
  }

  private SchemaTableName getSchemaTableFromPath(String schemaTablePath){
    List<String> parts = nsSplitter.splitToList(schemaTablePath);
    return new SchemaTableName(parts.get(0), parts.get(1));
  }

  @Override
  public void close() throws IOException {
    if (curator.getState() != CuratorFrameworkState.STOPPED)
      curator.create();
  }
}
