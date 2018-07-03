package io.panyu.skydrill.plugin.jdbc.metastore;

import io.airlift.configuration.Config;

public class ZkJdbcMetastoreConfig
{
  private String connectString = System.getProperty("zookeeper.connect.string");
  private String viewRootPath = "/skydrill/views";

  public String getConnectString() {
    return connectString;
  }

  @Config("connect.string")
  public ZkJdbcMetastoreConfig setConnectString(String connectString) {
    this.connectString = connectString;
    return this;
  }

  public String getViewRootPath() {
    return viewRootPath;
  }

  @Config("view.root.path")
  public ZkJdbcMetastoreConfig setViewRootPath(String rootPath) {
    this.viewRootPath = rootPath;
    return this;
  }
}
