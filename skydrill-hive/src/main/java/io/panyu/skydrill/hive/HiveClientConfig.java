package io.panyu.skydrill.hive;

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class HiveClientConfig
        extends com.facebook.presto.hive.HiveClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private String adlOAuth2RefreshUrl;
    private String adlOAuth2ClientId;
    private String adlOAuth2ClientCredential;

    private List<String> azureBlobAccounts = Collections.emptyList();
    private List<String> azureBlobAccountKeys = Collections.emptyList();

    public String getAdlOAuth2RefreshUrl() {
        return adlOAuth2RefreshUrl;
    }

    @Config("adl.oauth2-refresh-url")
    public HiveClientConfig setAdlOAuth2RefreshUrl(String refeshUrl) {
        this.adlOAuth2RefreshUrl = refeshUrl;
        return this;
    }

    public String getAdlOAuth2ClientId() {
        return adlOAuth2ClientId;
    }

    @Config("adl.oauth2-client-id")
    public HiveClientConfig setAdlOAuth2ClientId(String clientId) {
        this.adlOAuth2ClientId = clientId;
        return this;
    }

    public String getAdlOAuth2ClientCredential() {
        return adlOAuth2ClientCredential;
    }

    @Config("adl.oauth2-credential")
    @ConfigSecuritySensitive
    public HiveClientConfig setAdlOAuth2ClientCredential(String clientCredential) {
        this.adlOAuth2ClientCredential = clientCredential;
        return this;
    }

    public List<String> getAzureBlobAccounts() {
        checkArgument(azureBlobAccounts.size() == azureBlobAccountKeys.size());
        return azureBlobAccounts;
    }

    @Config("azure-blob.accounts")
    public HiveClientConfig setAzureBlobAccounts(String blobAccounts) {
        this.azureBlobAccounts = (blobAccounts == null)? null : SPLITTER.splitToList(blobAccounts);
        return this;
    }

    public List<String> getAzureBlobAccountKeys() {
        checkArgument(azureBlobAccounts.size() == azureBlobAccountKeys.size());
        return azureBlobAccountKeys;
    }

    @Config("azure-blob.account-keys")
    @ConfigSecuritySensitive
    public HiveClientConfig setAzureBlobAccountKeys(String blobAccountKeys) {
        this.azureBlobAccountKeys = (blobAccountKeys == null)? null : SPLITTER.splitToList(blobAccountKeys);
        return this;
    }
}
