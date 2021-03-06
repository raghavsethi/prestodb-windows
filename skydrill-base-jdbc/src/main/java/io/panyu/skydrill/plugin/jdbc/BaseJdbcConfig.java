package io.panyu.skydrill.plugin.jdbc;

import io.airlift.configuration.Config;

public class BaseJdbcConfig
        extends com.facebook.presto.plugin.jdbc.BaseJdbcConfig {

    public static final String PD_SUFFIX = "_";
    
    private boolean viewPushdownEnabled = false;

    public boolean isViewPushdownEnabled() {
        return viewPushdownEnabled;
    }

    @Config("jdbc.view-pushdown.enabled")
    public BaseJdbcConfig setViewPushdownEnabled(boolean enabled) {
        this.viewPushdownEnabled = enabled;
        return this;
    }
}
