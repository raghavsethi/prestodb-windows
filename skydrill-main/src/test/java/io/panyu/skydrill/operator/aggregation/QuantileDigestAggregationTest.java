package io.panyu.skydrill.operator.aggregation;

import org.testng.annotations.Test;

public class QuantileDigestAggregationTest
        extends AggregationTestFramework
{
    @Test
    public void testToDigest() {
        assertQuery("select percentile(make_digest(quantity),0.5) from tpch.tiny.lineitem", "select 25");
    }

    @Test
    public void testToDigest2() {
        assertQuery("select percentiles(make_digest(quantity),array[0.25,0.5,0.75])[3] from tpch.tiny.lineitem", "select 38.0");
    }
}