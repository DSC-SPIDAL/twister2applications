package edu.iu.dsc.tws.flinkapps.batch;

import edu.iu.dsc.tws.flinkapps.data.Generator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

public class AllReduce {
  private int size;
  private int iterations;
  private ExecutionEnvironment env;
  private String outFile;

  public AllReduce(int size, int iterations, ExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public void execute() {
    DataSet<String> stringStream = Generator.generateStringSet(env, size, iterations);

    DataSet<String> reduce = stringStream.map(new RichMapFunction<String, String>() {
      @Override
      public String map(String s) throws Exception {
        return s;
      }
    }).reduce(new RichReduceFunction<String>() {
      @Override
      public String reduce(String s, String t1) throws Exception {
        return t1;
      }
    });

    DataSet<Integer> s = Generator.generateOneElementDataSet(env);
    s.map(new RichMapFunction<Integer, Integer>() {
      @Override
      public Integer map(Integer integer) throws Exception {
        return integer;
      }
    }).withBroadcastSet(reduce, "reduce");

    reduce.writeAsText(outFile, FileSystem.WriteMode.OVERWRITE);
  }
}
