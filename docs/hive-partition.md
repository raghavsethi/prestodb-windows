## **Managing Table Partitions**

This applies specifically to table partition CRUD operations on unmanaged (or external) Hive tables. Skydrill allows adding/dropping table partitions using connector procedures as well as with the command line tool: HivePartitionUtils.

### **Hive Procedures**

When connected with the `skydrill-hive` connector, you can add or drop partitions using connector procedures `add_partition`, `add_partitions` and `drop_partition`. The partition value must be in the form that matches the layout of the external data. The procedures support the canonical hive partition layout `'year=2018/month=07/day=06/hour=01'` as well as the simplified partition layout `'2018/07/06/01'`.   
                                                                                                                                                                     
Example usage:

Adding partition:
    
     presto:measures> call schema.add_partition('measures', 'venice_staging', '2018/07/06/01');
     CALL
     presto:measures> select * from "venice_staging$partitions";
      year | month | day | hour
     ------+-------+-----+------
      2018 | 07    | 06  | 00
      2018 | 07    | 06  | 01
      2018 | 07    | 07  | 00
     
Dropping partition:

    presto:measures> call schema.drop_partition('measures', 'venice_staging', '2018/07/06/01');
    CALL
    presto:measures> select * from "venice_staging$partitions";
     year | month | day | hour
    ------+-------+-----+------
     2018 | 07    | 06  | 00
     2018 | 07    | 07  | 00    

Adding partitions in bulk:

    presto:measures> call schema.add_partitions('measures', 'venice_staging', '2018/07/06/00', '2018/07/06/23', 'yyyy/MM/dd/HH');
    CALL

Only hourly partition is supported for bulk operation. Again the format must match the the layout of the external data. Use the `'year='yyyy/'month='MM/'day='dd/'hour'=HH` formatter if the partition directory is of a key=value kind. 

### **HivePartitionUtils**

For Windows, the partition command is built into bin\skydrill.cmd. The usage is as follows:

    bin\skydrill partition
    usage: HivePartitionUtils [add|drop|list|update] catalog.schema.table [partition] [location]

For Linux/OSX, you can invoke via the class: io.panyu.skydrill.hive.HivePartitionUtils.

Listing partition on a table:

    bin\skydrill partition list hive.azure.venice_staging
    year=2018/month=05/day=01/hour=00,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/00
    year=2018/month=05/day=01/hour=01,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/01
    year=2018/month=05/day=01/hour=02,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/02
    year=2018/month=05/day=01/hour=03,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/03
    year=2018/month=05/day=01/hour=04,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/04
    year=2018/month=05/day=01/hour=05,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/05
    year=2018/month=05/day=01/hour=06,wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/05/01/06
    ...

Adding a partition:

    bin\skydrill partition add hive.azure.venice_staging "2018/07/18/10"
    added 2018/07/18/10

Adding partitions in bulk (from a file):

    bin\skydrill partition add hive.azure.venice_staging @partitions.txt
    added 2018/07/18/11
    added 2018/07/18/12
    added 2018/07/18/13
    ...

Dropping a partition:

    bin\skydrill partition drop hive.azure.venice_staging "2018/07/18/13"
    dropped 2018/07/18/13

Show location of external partition:

    bin\skydrill partition get hive.azure.venice_staging "2018/07/18/12"
    wasbs://venicestaging@wdgcoremeasures.blob.core.windows.net/WindowsCoreMeasures/2018/07/18/12
