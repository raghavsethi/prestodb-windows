package io.panyu.skydrill.plugin.jdbc.metastore;

import io.airlift.configuration.Config;

public class ZkJdbcMetastoreConfig
{
  private String connectString = System.getProperty("zookeeper.connect-string");
  private String catalogRootPath = "/skydrill/catalogs";

  public String getConnectString() {
    return connectString;
  }

  @Config("zookeeper.connect-string")
  public ZkJdbcMetastoreConfig setConnectString(String connectString) {
    this.connectString = connectString;
    return this;
  }

  public String getCatalogRootPath() {
    return catalogRootPath;
  }

  @Config("catalog.root-path")
  public ZkJdbcMetastoreConfig setCatalogRootPath(String rootPath) {
    this.catalogRootPath = rootPath;
    return this;
  }
}
