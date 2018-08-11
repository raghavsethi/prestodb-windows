package io.panyu.skydrill.operator.aggregation;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MergeQuantileDigestAggregationTest
        extends AggregationTestFramework
{
    @Test
    public void testMergeDigest() {
        assertQuery("select percentile(merge_digest(digest), 0.5) from (select make_digest(quantity) digest from tpch.tiny.lineitem group by shipmode) x", "select 25");
    }

    @Test
    public void testMergeDigest2() {
        assertQuery("select percentiles(merge_digest(digest), array[0.75])[1] from (select make_digest(quantity) digest from tpch.tiny.lineitem group by shipmode) x", "select 38.0");
    }

}