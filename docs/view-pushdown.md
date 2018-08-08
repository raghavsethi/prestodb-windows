## **Aggregation Push-down**

This applies to JDBC and Thrift based connectors. If the backing store supports aggregation, you have the option of pushing aggregation computing down to the connector level to improve overall query performance. This is accomplished via the Skydrill view push-down feature.

### Configuration

View push-down can be enabled by appending a hint (an underscore "_") to the view name when querying. View push-down can also be enabled explicitly using the config property  `jdbc.view-pushdown.enabled`. Push-down view and view can be used mix matched, that is, calling a push-down view from a normal view is supported.

Take this below view as an example:

    create view counttrains as select train, count(train) count from devicegrain_adoption where dmsloaddate = timestamp '2018-07-11' group by train;

Execute query against this view:

    select * from counttrains order by train;

      train  |   count
    ---------+-----------
     CB      |  88988857
     CBB     |  18443256
     Invalid |    982399
     LTSB    |  24784152
     LTSC    |    776780
     SAC     |  26066422
     SACT    | 468610177
     Unknown |   9627512
     WIP     |   2839617
    (9 rows)

    Query 20180807_000823_00003_9yfvg, FINISHED, 69 nodes
    Splits: 3,566 total, 3,566 done (100.00%)
    1:19 [641M rows, 0B] [8.15M rows/s, 0B/s]

Note this query scanned 641 million rows and took 1:19 sec. Now execute the same query using push-down view:

    select train, sum(count) count from counttrains_ group by train order by train;

      train  |   count
    ---------+-----------
     CB      |  88988857
     CBB     |  18443256
     Invalid |    982399
     LTSB    |  24784152
     LTSC    |    776780
     SAC     |  26066422
     SACT    | 468610177
     Unknown |   9627512
     WIP     |   2839617
    (9 rows)

    Query 20180807_001150_00005_9yfvg, FINISHED, 69 nodes
    Splits: 3,566 total, 3,566 done (100.00%)
    0:04 [1.66K rows, 0B] [424 rows/s, 0B/s]

The query time is now reduced to 4 sec. An extra group by is required to aggregate the push down query results from each shards of the table.

Create a view to encapsulate the outter group by above.

    create view counttrains2 as select train, sum(count) count from counttrains_ group by train;

Execute query against the new view:

    select * from counttrains2 order by train;

      train  |   count
    ---------+-----------
     CB      |  88988857
     CBB     |  18443256
     Invalid |    982399
     LTSB    |  24784152
     LTSC    |    776780
     SAC     |  26066422
     SACT    | 468610177
     Unknown |   9627512
     WIP     |   2839617
    (9 rows)

    Query 20180807_001850_00007_9yfvg, FINISHED, 69 nodes
    Splits: 3,566 total, 3,566 done (100.00%)
    0:04 [1.66K rows, 0B] [398 rows/s, 0B/s]

### Setting connector default:

    connector-name=skydrill-sqlserver
    jdbc.view-pushdown.enabled=true
    ...

Inspect session default:

    show session;
                      Name                       |     Value      |    Default     |  Type   |               Description
    ---------------------------------------------+----------------+----------------+---------+----------------------------------------
    sql.view_pushdown_enabled                    | false          | false          | boolean | Enable view to execute at the connector

Override session default:

    set session sql.view_pushdown_enabled=true;

Inspect change:

    show session;
                      Name                       |     Value      |    Default     |  Type   |               Description
    ---------------------------------------------+----------------+----------------+---------+----------------------------------------
    sql.view_pushdown_enabled                    | true           | false          | boolean | Enable view to execute at the connector

