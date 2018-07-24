## **Aggregation Push-down**

This applies to JDBC and Thrift based connectors. If the backing store supports aggregation, you have the option of pushing aggregation computing down to the connector level to improve overall query performance. This is accomplished via the Skydrill view push-down feature.

### Configuration

The view push-down feature can be enabled from session, or in the connector's configuration using the property `jdbc.view-pushdown.enabled`. The default setting for connector is `false`.

### Example:

&nbsp;&nbsp;Setting connector default:

    connector-name=skydrill-sqlserver
    jdbc.view-pushdown.enabled=true
    ...

&nbsp;&nbsp;Runtime override can be done using the `view_pushdown_enabled` session setting, as below:

    show session;
                      Name                       |     Value      |    Default     |  Type   |               Description
    ---------------------------------------------+----------------+----------------+---------+----------------------------------------
    sql.view_pushdown_enabled                    | false          | false          | boolean | Enable view to execute at the connector

&nbsp;&nbsp;Session override:

    set session sql.view_pushdown_enabled=true;

&nbsp;&nbsp;Verify change:

    show session;
                      Name                       |     Value      |    Default     |  Type   |               Description
    ---------------------------------------------+----------------+----------------+---------+----------------------------------------
    sql.view_pushdown_enabled                    | true           | false          | boolean | Enable view to execute at the connector
    