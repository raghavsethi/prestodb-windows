package io.panyu.skydrill.plugin.jdbc.metastore;

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class ZkJdbcMetastoreTest {

    private TestingServer zk;
    private ZkJdbcMetastore metastore;
    private SchemaTableName viewName;
    private ViewDefinition viewDefinition;
    private String viewData;

    @BeforeClass
    public void setUp()
            throws Exception {
        zk = new TestingServer(2181, true);
        System.setProperty("zookeeper.connect-string", "127.0.0.1:2181");
        metastore = new ZkJdbcMetastore(new JdbcConnectorId("test"), new ZkJdbcMetastoreConfig());
        viewName = new SchemaTableName("testschema", "testview");
        viewDefinition = new ViewDefinition("select foo from bar",
                Optional.of("cat"), Optional.of("test"),
                Collections.singletonList(new ViewDefinition.ViewColumn("count", BIGINT)),
                Optional.of("me"));
        viewData = metastore.getViewCodec().toJson(viewDefinition);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception {
        metastore.close();
        zk.close();
    }

    @Test
    public void testCreateView() {
        metastore.createView(viewName, viewData, true);
        assertEquals(metastore.isView(viewName), true);
    }

    @Test
    public void testDropView() {
        metastore.dropView(viewName);
        assertEquals(metastore.isView(viewName), false);
    }

    @Test
    public void testListViews() {
        metastore.createView(viewName, viewData, true);
        List<SchemaTableName> views = metastore.listViews("testschema");
        assertEquals(views, Collections.singletonList(viewName));
    }

    @Test
    public void testGetViewData() {
        metastore.createView(viewName, viewData, true);
        assertEquals(metastore.getViewData(viewName), "{\"originalSql\":\"select foo from bar\",\"catalog\":\"cat\",\"schema\":\"test\",\"columns\":[{\"name\":\"count\",\"type\":\"bigint\"}],\"owner\":\"me\"}");
    }

    @Test
    public void testGetViewDefinition() {
        metastore.createView(viewName, viewData, true);
        assertEquals(metastore.getViewDefinition(viewName).toString(), viewDefinition.toString());
    }

    @Test
    public void testIsView() {
        SchemaTableName foo = new SchemaTableName("testSchema", "foo");
        assertEquals(metastore.isView(foo), false);
    }
}