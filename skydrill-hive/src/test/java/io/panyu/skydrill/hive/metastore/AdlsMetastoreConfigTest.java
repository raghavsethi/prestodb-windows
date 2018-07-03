package io.panyu.skydrill.hive.metastore;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AdlsMetastoreConfigTest {

    @Test
    public void testGetAccount() {
    }

    @Test
    public void testSetAccount() {
        AdlsMetastoreConfig config = new AdlsMetastoreConfig()
                .setAccount("zoo");

        assertEquals(config.getAccount(), "zoo");
    }

    @Test
    public void testGetCatalogDirectory() {
        AdlsMetastoreConfig config = new AdlsMetastoreConfig()
                .setAccount("foo")
                .setCatalogDirectory("/bar");

        assertEquals(config.getCatalogDirectory(), "adl://foo.azuredatalakestore.net/bar");
    }

    @Test
    public void testSetCatalogDirectory() {
        AdlsMetastoreConfig config = new AdlsMetastoreConfig()
                .setAccount("zoo")
                .setCatalogDirectory("adl://foo.azuredatalakestore.net/bar");

        assertEquals(config.getCatalogDirectory(), "adl://foo.azuredatalakestore.net/bar");
    }
}