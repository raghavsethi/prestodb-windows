package io.panyu.skydrill.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class SqlServerClientConfig
{
  private String connectionStringBuilderTemplate = "jdbc:sqlserver://%s:%s;databaseName=%s";
  private String authenticationScheme;
  private String selectMethod = "direct";
  private int fetchSize = 1000;
  private boolean integratedSecurityEnabled;
  private String user;
  private String password;

  private boolean encryptEnabled;
  private boolean trustServerCertificate;
  private String hostNameInCertificate;
  private int loginTimeout;

  public String getAuthenticationScheme() {
    return authenticationScheme;
  }

  @Config("sqlserver.authentication-scheme")
  public SqlServerClientConfig setAuthenticationScheme(String authenticationScheme) {
    this.authenticationScheme = authenticationScheme;
    return this;
  }

  public String getSelectMethod() {
    return selectMethod;
  }

  @Config("sqlserver.select-method")
  public SqlServerClientConfig setSelectMethod(String selectMethod) {
    this.selectMethod = selectMethod;
    return this;
  }

  public int getFetchSize(){
    return fetchSize;
  }

  @Config("sqlserver.fetch-size")
  public SqlServerClientConfig setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  public boolean isIntegratedSecurityEnabled() {
    return integratedSecurityEnabled;
  }

  @Config("sqlserver.integrated-security")
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

  public String getConnectionStringBuilderTemplate() {
    return connectionStringBuilderTemplate;
  }

  @Config("sqlserver.connection-string-builder-template")
  public SqlServerClientConfig setConnectionStringBuilderTemplate(String template) {
    this.connectionStringBuilderTemplate = template;
    return this;
  }

  public boolean isEncryptEnabled() {
    return encryptEnabled;
  }

  @Config("sqlserver.encrypt")
  public SqlServerClientConfig setEncryptEnabled(boolean enabled) {
    this.encryptEnabled = enabled;
    return this;
  }

  public boolean isTrustServerCertificate() {
    return trustServerCertificate;
  }

  @Config("sqlserver.trustServerCertificate")
  public SqlServerClientConfig setTrustServerCertificate(boolean enabled) {
    this.trustServerCertificate = enabled;
    return this;
  }

  public String getHostNameInCertificate() {
    return hostNameInCertificate;
  }

  @Config("sqlserver.hostNameInCertificate")
  public SqlServerClientConfig setHostNameInCertificate(String hostname) {
    this.hostNameInCertificate = hostname;
    return this;
  }

  public int getLoginTimeout() {
    return loginTimeout;
  }

  @Config("sqlserver.loginTimeout")
  public SqlServerClientConfig setLoginTimeout(int timeout) {
    this.loginTimeout = timeout;
    return this;
  }

}
