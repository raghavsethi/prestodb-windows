package io.panyu.skydrill.operator.scalar;

import io.panyu.skydrill.operator.aggregation.AggregationTestFramework;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class QuantileDigestFunctionsTest
        extends AggregationTestFramework
{
    @Test
    public void testCount() {
        assertQuery("select digest_count(make_digest(quantity)) from tpch.tiny.lineitem", "select 60175");
    }

    @Test
    public void testError() {
        assertQuery("select digest_error(make_digest(quantity)) from tpch.tiny.lineitem", "select 0.01");
    }

    @Test
    public void testMax() {
        assertQuery("select digest_max(make_digest(quantity)) from tpch.tiny.lineitem", "select 50");
    }

    @Test
    public void testMin() {
        assertQuery("select digest_min(make_digest(quantity)) from tpch.tiny.lineitem", "select 1");
    }

    @Test
    public void testLower() {
        assertQuery("select digest_lower(make_digest(quantity), 0.5) from tpch.tiny.lineitem", "select 25");
    }

    @Test
    public void testUpper() {
        assertQuery("select digest_upper(make_digest(quantity), 0.75) from tpch.tiny.lineitem", "select 38");
    }

}