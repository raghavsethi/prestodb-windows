package io.panyu.skydrill.hive.metastore;

import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import io.panyu.skydrill.hive.HdfsConfigurationUpdater;
import io.panyu.skydrill.hive.HiveClientConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AzureBlobMetastoreTest {

    @BeforeMethod
    public void setUp() {
    }

    @AfterMethod
    public void tearDown() {
    }

    @Test
    public void testAzureBlobMetastore() {
        AzureBlobMetastoreConfig config = new AzureBlobMetastoreConfig();
        HiveClientConfig hiveConfig = new HiveClientConfig();
        S3ConfigurationUpdater s3updater = new PrestoS3ConfigurationUpdater(new HiveS3Config());
        HdfsConfigurationUpdater updater = new HdfsConfigurationUpdater(hiveConfig, s3updater);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updater);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        ExtendedHiveMetastore store = new AzureBlobMetastore(hdfsEnvironment, config);

        assertEquals(config.getMetastoreType(), "blob");
    }
}