## **Managing Table Partitions**

This applies specifically to table partition CRUD operations on unmanaged (or external) Hive tables. Skydrill allows adding/dropping table partitions with the command line tool: HivePartitionUtils, or
in session with the Skydrill Hive connector's custom procedures.

### **HivePartitionUtils**

For Windows, the partition command is built into bin\skydrill.cmd. The usage is as follows:

    bin\skydrill partition
    usage: HivePartitionUtils [add|drop|list|update] catalog.schema.table [partition] [location]

For Linux/OSX, you can invoke via the class: io.panyu.skydrill.hive.HivePartitionUtils.

Listing partition on a table:

    bin\skydrill partition list azure.measures.rome_staging
    year=2018/month=05/day=01/hour=00,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/00
    year=2018/month=05/day=01/hour=01,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/01
    year=2018/month=05/day=01/hour=02,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/02
    year=2018/month=05/day=01/hour=03,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/03
    year=2018/month=05/day=01/hour=04,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/04
    year=2018/month=05/day=01/hour=05,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/05
    year=2018/month=05/day=01/hour=06,wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/06
    ...

Adding a partition:

    bin\skydrill partition add azure.measures.rome_staging "2018/07/18/10"
    added 2018/07/18/10

Adding partitions in bulk (from a file):

    bin\skydrill partition add azure.measures.rome_staging @partitions.txt
    added 2018/07/18/11
    added 2018/07/18/12
    added 2018/07/18/13
    ...

Dropping a partition:

    bin\skydrill partition drop azure.measures.rome_staging "2018/07/18/13"
    dropped 2018/07/18/13

Show location of external partition:

    bin\skydrill partition get azure.measures.rome_staging "2018/07/18/12"
    wasbs://romestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/07/18/12

### **Hive Procedures**

When connected with the `skydrill-hive` connector, you can add or drop partitions using the procedure `add_partition` and `drop_partition` in session.

Example usage:

Adding partition:

     presto:measures> call schema.add_partition('measures', 'rome_staging', '2018/07/06/01');
     CALL
     presto:measures> select * from "rome_staging$partitions";
      year | month | day | hour
     ------+-------+-----+------
      2018 | 07    | 06  | 00
      2018 | 07    | 06  | 01
      2018 | 07    | 07  | 00

Dropping partition:

    presto:measures> call schema.drop_partition('measures', 'rome_staging', '2018/07/06/01');
    CALL
    presto:measures> select * from "rome_staging$partitions";
     year | month | day | hour
    ------+-------+-----+------
     2018 | 07    | 06  | 00
     2018 | 07    | 07  | 00

The partition value must be in the form that matches the layout of the external data. Only the canonical Hive partition layout ('year=2018/month=07/day=06/hour=01') and the simplified layout ('2018/07/06/01') are supported.
