package io.panyu.skydrill.hive;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

public class HiveProceduresTest {

    @Test
    public void testGetPartitions() {
        HivePartitionUtils utils = new HivePartitionUtils();
        String range = "2018/08/01/00-2018/08/01/02";
        String format = "yyyy/MM/dd/HH";
        List<String> list = utils.getPartitionEntries(range, format);
        assertEquals(list, ImmutableList.of("2018/08/01/00", "2018/08/01/01", "2018/08/01/02"));
    }

    @Test
    public void testGetHivePartitions() {
        HivePartitionUtils utils = new HivePartitionUtils();
        String range = "year=2018/month=08/day=01/hour=00-year=2018/month=08/day=01/hour=02";
        String format = "'year='yyyy/'month='MM/'day='dd/'hour'=HH";
        List<String> list = utils.getPartitionEntries(range, format);
        assertEquals(list, ImmutableList.of("year=2018/month=08/day=01/hour=00",
                "year=2018/month=08/day=01/hour=01",
                "year=2018/month=08/day=01/hour=02"));
    }

    @Test
    public void testGetPartitionsCrossDay() {
        HivePartitionUtils utils = new HivePartitionUtils();
        String range = "2018/08/01/23-2018/08/02/01";
        String format = "yyyy/MM/dd/HH";
        List<String> list = utils.getPartitionEntries(range, format);
        assertEquals(list, ImmutableList.of("2018/08/01/23", "2018/08/02/00", "2018/08/02/01"));
    }

}