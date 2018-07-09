package io.panyu.skydrill.connector.thrift;

import io.panyu.skydrill.plugin.jdbc.BaseJdbcConfig;
import io.panyu.skydrill.plugin.jdbc.JdbcSessionProperties;

import javax.inject.Inject;

public class SkydrillThriftSessionProperties
        extends JdbcSessionProperties
{
    @Inject
    public SkydrillThriftSessionProperties() {
        super(new BaseJdbcConfig());
    }
}
