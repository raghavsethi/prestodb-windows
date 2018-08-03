package io.panyu.skydrill.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.XX_HASH_64;

@AggregationFunction("to_hll")
public final class DefaultHyperLogLogAggregation {
  private static final double DEFAULT_STANDARD_ERROR = 0.023;

  private DefaultHyperLogLogAggregation() {
  }

  @InputFunction
  @TypeParameter("T")
  public static void input(
          @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
          @AggregationState HyperLogLogState state,
          @SqlType("T") long value) {
    HyperLogLogAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
  }

  @InputFunction
  @TypeParameter("T")
  public static void input(
          @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
          @AggregationState HyperLogLogState state,
          @SqlType("T") double value) {
    HyperLogLogAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
  }

  @InputFunction
  @TypeParameter("T")
  public static void input(
          @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
          @AggregationState HyperLogLogState state,
          @SqlType("T") Slice value) {
    HyperLogLogAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
  }

  @CombineFunction
  public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState) {
    HyperLogLogAggregation.combineState(state, otherState);
  }

  @OutputFunction(StandardTypes.HYPER_LOG_LOG)
  public static void output(@AggregationState HyperLogLogState state, BlockBuilder out) {
    HyperLogLogAggregation.output(state, out);
  }
}
