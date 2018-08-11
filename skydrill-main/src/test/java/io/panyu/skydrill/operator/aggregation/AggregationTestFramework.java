package io.panyu.skydrill.operator.aggregation;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import io.panyu.skydrill.operator.scalar.QuantileDigestFunctions;

import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class AggregationTestFramework
        extends AbstractTestQueryFramework
{
    protected AggregationTestFramework() {
        super(AggregationTestFramework::createLocalQueryRunner);
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1, true), ImmutableMap.of());

        List<SqlFunction> functions = new FunctionListBuilder()
                .aggregate(HyperLogLogAggregation.class)
                .aggregate(DefaultHyperLogLogAggregation.class)
                .aggregates(MergeQuantileDigestAggregation.class)
                .aggregates(QuantileDigestAggregation.class)
                .scalars(QuantileDigestFunctions.class)
                .getFunctions();

        localQueryRunner.getMetadata().addFunctions(functions);

        return localQueryRunner;
    }
}
