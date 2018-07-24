## **Azure Blob Connector**

The Skydrill Azure Blob connector allows querying data stored in Azure Blob storage. The connector is based on the Presto Hive connector with semantics similar to its Amazon S3 support. The Skydrill connector comes with added feature of an Azure Blob catalog store, allowing you to quickly onboard your data without the need for a thrift based metastore.

### Configuration

Create etc/catalog/blob.properties with the following contents to mount the skydrill-hive connector as the catalog for your blob data, replacing `mycontainer` and `myblobstorageaccount` and access key(s) with the actual ones for your data:

    connector.name=skydrill-hive
    hive.metastore=blob
    hive.metastore.catalog-dir=wasbs://mycontainer@myblobaccount.blob.core.windows.net/mydatabase/catalog
    azure-blob.accounts=myblobstorageaccount,myblobstorageaccount1,myblobstorageaccount2
    azure-blob.account-keys=account1accesskey,account2accesskey,account3accesskey

### Storage Account Configuration

Storage account information can be done either inline in the catalog properties file or in a dedicated resource file. For example, adding the line:

    hive.config.resources=etc/hive/core-site.xml

loads the account information from the `etc/hive/core-site.xml` file. The content of the file should follow this convention below:

    <configuration>
      <property>
        <name>fs.azure.account.key.myblobstorageaccount.blob.core.windows.net</name>
        <value>your access key</value>
     </property>
    </configuration>

You can specify multiple storage accounts in the same file.

### Examples

Create a new blob schema named web that will store tables in a container named mycontainer:

    CREATE SCHEMA blob.web
    WITH (location = 'wasbs://mycontainer@myblobaccount.blob.core.windows.net/mydatabase')

Create a new table named page_views in the web schema that is stored using the ORC file format, partitioned by date and country, and bucketed by user into 50 buckets (note that Hive requires the partition columns to be the last columns in the table):    

    CREATE TABLE hive.web.page_views (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioned_by = ARRAY['ds', 'country'],
      bucketed_by = ARRAY['user_id'],
      bucket_count = 50
    )

Drop a partition from the page_views table:

    DELETE FROM hive.web.page_views
    WHERE ds = DATE '2016-08-09'
      AND country = 'US'
      
Query the page_views table:

    SELECT * FROM hive.web.page_views

List the partitions of the page_views table:

    SELECT * FROM hive.web."page_views$partitions"

Create an external table named request_logs that points at existing data in Azure blob:

    CREATE TABLE blob.web.request_logs (
      request_time timestamp,
      url varchar,
      ip varchar,
      user_agent varchar
    )
    WITH (
      format = 'TEXTFILE',
      external_location = 'wasbs://mycontainer@myblobaccount.blob.core.windows.net/data/logs'
    )

Drop the external table request_logs. This only drops the metadata for the table. The referenced data directory is not deleted:

    DROP TABLE blob.web.request_logs

Drop a schema:

    DROP SCHEMA blob.web

### Limitations

DELETE is only supported if the WHERE clause matches entire partitions.
