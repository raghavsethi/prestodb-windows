package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.google.inject.Inject;

public class JdbcMetadataFactory
        extends com.facebook.presto.plugin.jdbc.JdbcMetadataFactory
{
  private final JdbcClient jdbcClient;
  private final boolean allowDropTable;

  @Inject
  public JdbcMetadataFactory(JdbcClient jdbcClient, JdbcMetadataConfig config) {
    super(jdbcClient, config);
    this.jdbcClient = jdbcClient;
    this.allowDropTable = config.isAllowDropTable();
  }

  public JdbcMetadata create() {
    return new JdbcMetadata(jdbcClient, allowDropTable);
  }
}
