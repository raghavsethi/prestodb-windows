package io.panyu.skydrill.metadata;

import io.airlift.configuration.Config;

public class SkydrillCatalogStoreConfig {
    private String catalogRootPath = "/skydrill/catalog";

    public String getCatalogRootPath() {
        return catalogRootPath;
    }

    @Config("catalog.config-root")
    public SkydrillCatalogStoreConfig setCatalogRootPath(String rootPath) {
        this.catalogRootPath = rootPath;
        return this;
    }
}
