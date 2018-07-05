package io.panyu.skydrill.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class BaseJdbcClientTest {
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private TestingDatabase database;
    private String catalogName;
    private JdbcClient jdbcClient;
    private SchemaTableName fooView = new SchemaTableName("example", "foo");

    @BeforeClass
    public void setUp()
            throws Exception {
        database = new TestingDatabase();
        catalogName = database.getConnection().getCatalog();
        jdbcClient = database.getJdbcClient();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        jdbcClient.dropView(session, fooView);
        database.close();
    }

    @Test
    public void testGetTableNames() {
        List<SchemaTableName> example = jdbcClient.getTableNames("example");
        assertEquals(example.size(), 3);
        jdbcClient.createView(session, fooView, "foobar", true);
        example = jdbcClient.getTableNames("example");
        assertEquals(example.size(), 4);
    }

    @Test
    public void testDropView() {
        jdbcClient.createView(session, fooView, "foobar", true);
        assertEquals(jdbcClient.listViews(session, "example").contains(fooView), true);
        jdbcClient.dropView(session, fooView);
        assertEquals(jdbcClient.listViews(session, "example").contains(fooView), false);
    }

    @Test
    public void testGetViews() {
        jdbcClient.createView(session, fooView, "foobar", true);
        assertEquals(jdbcClient.listViews(session, "example").contains(fooView), true);
        Map<SchemaTableName, ConnectorViewDefinition> example = jdbcClient.getViews(session, new SchemaTablePrefix("example"));
        assertEquals(example.get(fooView).getViewData(), "foobar");
    }
}