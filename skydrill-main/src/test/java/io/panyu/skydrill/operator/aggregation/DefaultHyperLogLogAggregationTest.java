package io.panyu.skydrill.operator.aggregation;

import org.testng.annotations.Test;

public class DefaultHyperLogLogAggregationTest
        extends AggregationTestFramework
{
    @Test
    public void testToHll() {
        assertQuery("select cardinality(to_hll(shipmode)) from tpch.tiny.lineitem", "select 7");
    }

    @Test
    public void testMergeHll() {
        assertQuery("select cardinality(merge(hll)) from (select to_hll(shipmode) hll from tpch.tiny.lineitem group by shipmode) x", "select 7");
    }

}