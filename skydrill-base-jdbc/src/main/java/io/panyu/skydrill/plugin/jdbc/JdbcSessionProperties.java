package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;

public class JdbcSessionProperties {
    private static final String VIEW_PUSHDOWN_ENABLED = "view_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public JdbcSessionProperties(BaseJdbcConfig config) {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(
                        VIEW_PUSHDOWN_ENABLED,
                        "Enable view to execute at the connector",
                        config.isViewPushdownEnabled(),
                        false)
        );
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isViewPushdownEnabled(ConnectorSession session) {
        return session.getProperty(VIEW_PUSHDOWN_ENABLED, Boolean.class);
    }
}
