## **Managing Table Partitions**

This applies specifically to partition CRUD operations on unmanaged (or external) Hive tables. Skydrill provides a bonus feature, HivePartitionUtils, to making adding / dropping partitions to unmanaged (or external) table easy. Managing partitions on unmanaged table is not directly support by Presto.

### **Usage**

The command line to bin\skydrill.cmd partition, or you can invoke via the io.panyu.skydrill.hive.HivePartitionUtils class.

    bin\skydrill partition
    usage: HivePartitionUtils [add|drop|list|update] catalog.schema.table [partition] [location]

### **Example Usage**

Listing partition on a table:

    bin\skydrill partition list hive.azure.venice_staging
    year=2018/month=05/day=01/hour=00,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/00
    year=2018/month=05/day=01/hour=01,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/01
    year=2018/month=05/day=01/hour=02,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/02
    year=2018/month=05/day=01/hour=03,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/03
    year=2018/month=05/day=01/hour=04,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/04
    year=2018/month=05/day=01/hour=05,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/05
    year=2018/month=05/day=01/hour=06,wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/05/01/06
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
    wasbs://venicestaging@acmemeasures.blob.core.windows.net/AcmeMeasures/2018/07/18/12
