package io.panyu.skydrill.hive.metastore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.panyu.skydrill.hive.HiveMetastoreConfig;

import javax.validation.constraints.NotNull;

public class AdlsMetastoreConfig
        extends HiveMetastoreConfig
{
    private String account;
    private String catalogDirectory = "/catalog";

    public String getAccount() {
        return account;
    }

    @Config("adl.metastore.account")
    @ConfigDescription("ADL metastore store account")
    public AdlsMetastoreConfig setAccount(String account) {
        this.account = account;
        return this;
    }

    public String getCatalogDirectory() {
        return normalizedCatalogDir();
    }

    @Config("hive.metastore.catalog-dir")
    @ConfigDescription("Hive metastore catalog directory")
    public AdlsMetastoreConfig setCatalogDirectory(String catalogDirectory)
    {
        this.catalogDirectory = catalogDirectory;
        return this;
    }

    private String normalizedCatalogDir()
    {
        if (catalogDirectory.startsWith("adl://"))
            return catalogDirectory;
        else
            return String.format("adl://%s.azuredatalakestore.net%s", getAccount(), catalogDirectory);
    }
}
