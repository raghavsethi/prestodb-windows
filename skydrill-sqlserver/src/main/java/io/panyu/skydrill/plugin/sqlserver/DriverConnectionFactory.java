package io.panyu.skydrill.plugin.sqlserver;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
  private final Driver driver;
  private final String connectionUrl;
  private final Properties connectionProperties;

  public DriverConnectionFactory(String connectionUrl, Properties connectionProperties)
  {
    this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
    this.connectionProperties = requireNonNull(connectionProperties, "basicConnectionProperties is null");
    this.driver = new SQLServerDriver();
  }

  public DriverConnectionFactory(BaseJdbcConfig config, SqlServerClientConfig clientConfig)
  {
    this(config.getConnectionUrl(), getDriverProperties(clientConfig));
  }

  @Override
  public Connection openConnection()
          throws SQLException
  {
    return driver.connect(connectionUrl, connectionProperties);
  }

  public Connection openConnection(String connectionUrl)
          throws SQLException
  {
    return driver.connect(connectionUrl, connectionProperties);
  }

  private static Properties getDriverProperties(SqlServerClientConfig clientConfig)
  {
      return new Properties() {{
          if (clientConfig.isIntegratedSecurityEnabled()) {
              put("integratedSecurity", "true");
          } else {
              put("user", requireNonNull(clientConfig.getUser(), "sqlserver.user is null"));
              put("password", requireNonNull(clientConfig.getPassword(), "sqlserver.password is null"));
          }

          if (clientConfig.getAuthenticationScheme() != null) {
              put("authenticationScheme", clientConfig.getAuthenticationScheme());
          }

          if (clientConfig.getSelectMethod() != null) {
              put("selectMethod", clientConfig.getSelectMethod());
          }

          put("encrypt", String.valueOf(clientConfig.isEncryptEnabled()));
          put("trustServerCertificate", String.valueOf(clientConfig.isTrustServerCertificate()));

          if (clientConfig.getLoginTimeout() > 0) {
              put("loginTimeout", String.valueOf(clientConfig.getLoginTimeout()));
          }

          if (clientConfig.getHostNameInCertificate() != null) {
              put("hostNameInCertificate", clientConfig.getHostNameInCertificate());
          }
      }};
  }
}
