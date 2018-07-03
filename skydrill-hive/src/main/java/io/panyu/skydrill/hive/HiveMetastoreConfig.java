package io.panyu.skydrill.hive;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class HiveMetastoreConfig {
    private String metastoreType = "blob";
    private String metastoreUser = "skydrill";

    @NotNull
    public String getMetastoreType()
    {
        return metastoreType;
    }

    @Config("hive.metastore")
    public HiveMetastoreConfig setMetastoreType(String metastoreType)
    {
        this.metastoreType = metastoreType;
        return this;
    }

    public String getMetastoreUser()
    {
        return metastoreUser;
    }

    @Config("hive.metastore.user")
    @ConfigDescription("Hive metastore username for file access")
    public HiveMetastoreConfig setMetastoreUser(String metastoreUser)
    {
        this.metastoreUser = metastoreUser;
        return this;
    }

}
