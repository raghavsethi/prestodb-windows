package io.panyu.skydrill.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class SqlServerClientConfig
{
  private String authenticationScheme;
  private String selectMethod = "direct";
  private int fetchSize = 1000;
  private boolean integratedSecurityEnabled;
  private String user;
  private String password;

  public String getAuthenticationScheme() {
    return authenticationScheme;
  }

  @Config("sqlserver.authentication.scheme")
  public SqlServerClientConfig setAuthenticationScheme(String authenticationScheme) {
    this.authenticationScheme = authenticationScheme;
    return this;
  }

  public String getSelectMethod() {
    return selectMethod;
  }

  @Config("sqlserver.select.method")
  public SqlServerClientConfig setSelectMethod(String selectMethod) {
    this.selectMethod = selectMethod;
    return this;
  }

  public int getFetchSize(){
    return fetchSize;
  }

  @Config("sqlserver.fetch.size")
  public SqlServerClientConfig setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  public boolean isIntegratedSecurityEnabled() {
    return integratedSecurityEnabled;
  }

  @Config("sqlserver.integrated.security")
  public SqlServerClientConfig setIntegratedSecurityEnabled(boolean enabled){
    this.integratedSecurityEnabled = enabled;
    return this;
  }

  public String getUser() {
    return user;
  }

  @Config("sqlserver.user")
  @ConfigSecuritySensitive
  public SqlServerClientConfig setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  @Config("sqlserver.password")
  @ConfigSecuritySensitive
  public SqlServerClientConfig setPassword(String password){
    this.password = password;
    return this;
  }
}
