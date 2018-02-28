package edu.iu.dsc.tws.flinkapps.iter;

import edu.iu.dsc.tws.flinkapps.data.IntegerListFormat;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.List;

public class ReduceIterative {
  private int size;
  private int iterations;
  private ExecutionEnvironment env;
  private String outFile;

  public ReduceIterative(int size, int iterations, ExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public void execute() {
    IterativeDataSet<List<Integer>> initial = env.createInput(new IntegerListFormat(size)).iterate(iterations);

    DataSet<List<Integer>> iteration = initial
        .map(new RichMapFunction<List<Integer>, List<Integer>>() {
          private LongSumAggregator agg = null;
          @Override
          public void open(Configuration parameters) {
            this.agg = this.getIterationRuntimeContext().getIterationAggregator("total");
          }
          @Override
          public List<Integer> map(
              List<Integer> input) throws Exception {
            return input;
          }
        }).reduce(new RichReduceFunction<List<Integer>>() {
          @Override
          public List<Integer> reduce(List<Integer> integers, List<Integer> t1) throws Exception {
            return t1;
          }
        });

    DataSet<List<Integer>>  finalData = initial.closeWith(iteration);

    try {
      finalData.print();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static DataSet<LongValue> getData(
      ExecutionEnvironment env) {
    return env.fromParallelCollection(new LongValueSequenceIterator(0, 1000), LongValue.class);
  }
}
