package io.panyu.skydrill.hive.metastore;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class AzureBlobMetastoreConfigTest {

    @Test
    public void testGetContainer() {
        AzureBlobMetastoreConfig config = new AzureBlobMetastoreConfig()
                .setContainer("foo@bar")
                .setCatalogDirectory("/cat");
        assertEquals(config.getContainer(), "foo@bar");
    }

    @Test
    public void testSetContainer() {
    }

    @Test
    public void testGetCatalogDirectory() {
        AzureBlobMetastoreConfig config = new AzureBlobMetastoreConfig()
                .setContainer("foo@bar")
                .setCatalogDirectory("/cat");
        assertEquals(config.getCatalogDirectory(), "wasbs://foo@bar.blob.core.windows.net/cat");
    }

    @Test
    public void testSetCatalogDirectory() {
        AzureBlobMetastoreConfig config = new AzureBlobMetastoreConfig()
                .setContainer("foo@bar")
                .setCatalogDirectory("wasbs://zoo@bar.blob.core.windows.net/cat/dog");
        assertEquals(config.getCatalogDirectory(), "wasbs://zoo@bar.blob.core.windows.net/cat/dog");
    }
}