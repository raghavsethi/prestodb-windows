package io.panyu.skydrill.metadata;

import io.airlift.configuration.Config;

public class SkydrillCatalogStoreConfig {
    private String catalogRootPath = "/skydrill/runtime/catalog";

    public String getCatalogRootPath() {
        return catalogRootPath;
    }

    @Config("runtime.catalog.root-path")
    public SkydrillCatalogStoreConfig setCatalogRootPath(String rootPath) {
        this.catalogRootPath = rootPath;
        return this;
    }
}
