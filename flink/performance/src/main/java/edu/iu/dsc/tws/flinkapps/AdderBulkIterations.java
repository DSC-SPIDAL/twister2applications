package edu.iu.dsc.tws.flinkapps;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.LongValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AdderBulkIterations implements java.io.Serializable {
  /*Each input number must be between 0 - INPUT_MAX (exclusive)*/
  public static int INPUT_MAX = 100;
  /*Number of elements in our toy dataset*/
  public static int NO_OF_ELEMENTS = 10;
  /*Iterations stop when sum of all numbers exceeds ABSOLUTE_MAX*/
  public static long ABSOLUTE_MAX =NO_OF_ELEMENTS * 20000;

  /*Maxium Iterations*/
  public static int MAX_ITERATIONS = 100000;

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    long initialTime = System.currentTimeMillis();
    // First create an initial dataset
    IterativeDataSet<Tuple2<Long, Long>> initial = getData(
        env).iterate(MAX_ITERATIONS);
    //Register Aggregator and Convergence Criterion Class
    initial.registerAggregationConvergenceCriterion("total", new LongSumAggregator(), new VerifyIfMaxConvergence());

    DataSet<Tuple2<Long, Long>> iteration = initial
        .map(new RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          private LongSumAggregator agg = null;
          @Override
          public void open(Configuration parameters) {
            this.agg = this.getIterationRuntimeContext().getIterationAggregator("total");
          }
          @Override
          public Tuple2<Long, Long> map(
              Tuple2<Long, Long> input) throws Exception {
            long incrementF1 = input.f1 + 1;
            Tuple2<Long, Long> out = new Tuple2<>(input.f0, incrementF1);
            this.agg.aggregate(out.f1);
            return out;
          }
        });

    DataSet<Tuple2<Long, Long>> finalDs = initial.closeWith(iteration);
    finalDs.print();
    System.out.println("Total time to run the job " + (System.currentTimeMillis()-initialTime));

  }

  public static DataSet<Tuple2<Long, Long>> getData(
      ExecutionEnvironment env) {
    List<Tuple2<Long, Long>> lst = new ArrayList<Tuple2<Long, Long>>();
    Random rnd = new Random();
    for (int i = 0; i < NO_OF_ELEMENTS; i++) {
      long r = rnd.nextInt(INPUT_MAX);
      lst.add(new Tuple2<Long, Long>(r, r));
    }
    return env.fromCollection(lst);
  }

  public static class VerifyIfMaxConvergence implements ConvergenceCriterion<LongValue> {
    @Override
    public boolean isConverged(int iteration, LongValue value) {
      return (value.getValue()>AdderBulkIterations.ABSOLUTE_MAX);
    }
  }
}