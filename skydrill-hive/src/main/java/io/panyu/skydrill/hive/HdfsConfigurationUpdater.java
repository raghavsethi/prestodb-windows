package io.panyu.skydrill.hive;

import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

public class HdfsConfigurationUpdater
        extends com.facebook.presto.hive.HdfsConfigurationUpdater
{
    private final HiveClientConfig hiveClientConfig;

    @Inject
    public HdfsConfigurationUpdater(HiveClientConfig config, S3ConfigurationUpdater s3ConfigurationUpdater) {
        super(config, s3ConfigurationUpdater);
        this.hiveClientConfig = config;
    }

    @Override
    public void updateConfiguration(Configuration config) {
        config.set("fs.adl.impl","org.apache.hadoop.fs.adl.AdlFileSystem");
        config.set("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
        config.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential");

        int processors = Runtime.getRuntime().availableProcessors();
        config.set("fs.azure.delete.threads", String.valueOf(processors));
        config.set("fs.azure.rename.threads", String.valueOf(processors));

        if (hiveClientConfig.getAdlOAuth2RefreshUrl() != null) {
            config.set("fs.adl.oauth2.refresh.url", hiveClientConfig.getAdlOAuth2RefreshUrl());
        }

        if (hiveClientConfig.getAdlOAuth2ClientId() != null) {
            config.set("fs.adl.oauth2.client.id", hiveClientConfig.getAdlOAuth2ClientId());
        }

        if (hiveClientConfig.getAdlOAuth2ClientCredential() != null) {
            config.set("fs.adl.oauth2.credential", hiveClientConfig.getAdlOAuth2ClientCredential());
        }

        for(int i = 0; i < hiveClientConfig.getAzureBlobAccounts().size(); i++) {
            config.set(String.format("fs.azure.account.key.%s.blob.core.windows.net", hiveClientConfig.getAzureBlobAccounts().get(i)),
                    hiveClientConfig.getAzureBlobAccountKeys().get(i));
        }

        super.updateConfiguration(config);
    }
}
