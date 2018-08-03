package io.panyu.skydrill.operator.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;

import java.util.List;

public final class QuantileDigestFunctions {
  private QuantileDigestFunctions(){
  }

  @ScalarFunction
  @Description("compute quantile from a QuantileDigest instance")
  @SqlType(StandardTypes.BIGINT)
  public static long quantile(@SqlType(StandardTypes.VARBINARY) Slice serializedQDigest,
                              @SqlType(StandardTypes.DOUBLE) double quantile)
  {
    return new QuantileDigest(serializedQDigest).getQuantile(quantile);
  }

  @ScalarFunction
  @Description("compute quantiles from a QuantileDigest instance")
  @SqlType("array(bigint)")
  public static List<Long> quantiles(@SqlType(StandardTypes.VARBINARY) Slice serializedQDigest,
                                     @SqlType("array(double)") List<Double> quantiles)
  {
    return new QuantileDigest(serializedQDigest).getQuantiles(quantiles);
  }
}
