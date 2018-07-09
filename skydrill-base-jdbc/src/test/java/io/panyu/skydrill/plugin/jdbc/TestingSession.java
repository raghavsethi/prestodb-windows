package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;

import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public final class TestingSession
{
    public static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return "test_query_id";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.of("TestSource");
        }

        @Override
        public Identity getIdentity()
        {
            return new Identity("user", Optional.empty());
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return UTC_KEY;
        }

        @Override
        public Locale getLocale()
        {
            return ENGLISH;
        }

        @Override
        public long getStartTime()
        {
            return 0;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Deprecated
        public boolean isLegacyTimestamp()
        {
            return true;
        }

        @Deprecated
        public boolean isLegacyRoundNBigint()
        {
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getProperty(String name, Class<T> type)
        {
            return (T) booleanSessionProperty(
                    "view_pushdown_enabled",
                    "Enable view to execute at the connector",
                    false,
                    false).getDefaultValue();
        }
    };

    private TestingSession() {}
}
