package io.panyu.skydrill.plugin.jdbc.metastore;

import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;

public class ZkJdbcMetastoreTest {

    private static final JsonCodecFactory jsonCodecFactory = new JsonCodecFactory();
    private TestingServer zk;
    private ZkJdbcMetastore metastore;
    private SchemaTableName viewName;
    private JsonCodec<ViewDefinition> viewCodec;
    private ViewDefinition viewDefinition;
    private String viewData;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        zk = new TestingServer(2181, true);
        System.setProperty("zookeeper.connect.string", "127.0.0.1:2181");
        metastore = new ZkJdbcMetastore(new JdbcConnectorId("test"), new ZkJdbcMetastoreConfig());
        viewName = new SchemaTableName("testschema", "testview");
        viewCodec = jsonCodecFactory.jsonCodec(ViewDefinition.class);
        viewDefinition = new ViewDefinition("select foo from bar",
                Optional.of("cat"), Optional.of("test"),
                Collections.singletonList(new ViewDefinition.ViewColumn("foo", INTEGER)),
                Optional.of("me"));
        viewData = viewCodec.toJson(viewDefinition);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
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
        assertEquals(metastore.getViewData(viewName), "{\"originalSql\":\"select foo from bar\",\"catalog\":\"cat\",\"schema\":\"test\",\"columns\":[{\"name\":\"foo\",\"type\":\"integer\"}],\"owner\":\"me\"}");
    }

    @Test
    public void testGetViewDefinition() {
        metastore.createView(viewName, viewData, true);
        assertEquals(metastore.getViewDefinition(viewName), viewDefinition);
    }

    @Test
    public void testIsView() {
        SchemaTableName foo = new SchemaTableName("testSchema", "foo");
        assertEquals(metastore.isView(foo), false);
    }
}