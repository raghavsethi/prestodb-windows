## **Azure Data Lake Connector**

This connector shares the same core as the Azure Blob connector. It allows querying data stored in Azure Data Lake Storage (ADLS) account.  The connector also come with the added feature of ADLS based metastore to allow you to quickly onboard data without a thrift based metastore.

### Configuration

Create etc/catalog/adls.properties with the following contents to mount the skydrill-hive connector as the catalog for your ADLS data, replacing myaccount and access key with the actual ones for your data:

    connector.name=skydrill-hive
    hive.metastore=adls
    hive.metastore.catalog-dir=adl://myaccount.azuredatalakestore.net/mydatabase/catalog
    adl.oauth2-refresh-url=https://login.microsoftonline.com/myappid/oauth2/token
    adl.oauth2-client-id=myclientid
    adl.oauth2-credential=mycredential

### Storage Account Configuration

Storage account information can be done either inline in the catalog properties file or in a dedicated resource file. For example, adding the line:

    hive.config.resources=etc/hive/core-site.xml

loads the account information from the etc/hive/core-site.xml file. The content of the file should follow this convention below:

    <configuration>
      <property>
        <name>fs.adl.oauth2.refresh.url</name>
        <value>https://login.microsoftonline.com/myappid/oauth2/token</value>
      </property>
      <property>
        <name>fs.adl.oauth2.client.id</name>
        <value>myclientid</value>
      </property>
      <property>
        <name>fs.adl.oauth2.credential</name>
        <value>mycredential</value>
      </property>
    </configuration>

### Examples

Create a new adls schema named datalake for mounting external tables:

    CREATE SCHEMA adls.datalake

Mount a external table to the datalake schema:

    create table adls.datalake.venice_staging (
      raw varchar,
      year varchar(4),
      month varchar(2),
      day varchar(2),
      hour varchar(2)
    ) with (
      format='textfile',
      partitioned_by=array['year', 'month', 'day', 'hour'],
      external_location='adl://myaccount.azuredatalakestore.net/DataAccess/AcmeMeasures');
    )

Add partition to the external table:

    bin\skydrill partition add adls.datalake.venice_staging "2018/05/01/00"
    bin\skydrill partition add adls.datalake.venice_staging "2018/05/01/01"
    bin\skydrill partition add adls.datalake.venice_staging "2018/05/01/03"

List the partitions of the page_views table:

    SELECT * FROM adls.datalake."venice_staging.$partitions"

### Limitations

DELETE is only supported if the WHERE clause matches entire partitions.