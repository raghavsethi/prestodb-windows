package io.panyu.skydrill.hive.metastore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.panyu.skydrill.hive.HiveMetastoreConfig;

import javax.validation.constraints.NotNull;

public class AzureBlobMetastoreConfig
        extends HiveMetastoreConfig
{
    private String container;
    private String catalogDirectory = "/catalog";

    public String getContainer() {
        return container;
    }

    @Config("hive.metastore.container")
    @ConfigDescription("Azure metastore blob container (container@account)")
    public AzureBlobMetastoreConfig setContainer(String container) {
        this.container = container;
        return this;
    }

    public String getCatalogDirectory() {
        return normalizedCatalogDir();
    }

    @Config("hive.metastore.catalog-dir")
    @ConfigDescription("Hive metastore catalog directory")
    public AzureBlobMetastoreConfig setCatalogDirectory(String catalogDirectory)
    {
        this.catalogDirectory = catalogDirectory;
        return this;
    }

    private String normalizedCatalogDir()
    {
       if (catalogDirectory.startsWith("wasbs://") || catalogDirectory.startsWith("wasb://"))
           return catalogDirectory;
       else
           return String.format("wasbs://%s.blob.core.windows.net%s", getContainer(), catalogDirectory);
    }

}
