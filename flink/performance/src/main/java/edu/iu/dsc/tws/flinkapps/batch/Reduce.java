package edu.iu.dsc.tws.flinkapps.batch;

import edu.iu.dsc.tws.flinkapps.data.Generator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

public class Reduce {
  private int size;
  private int iterations;
  private ExecutionEnvironment env;
  private String outFile;

  public Reduce(int size, int iterations, ExecutionEnvironment env, String outFile) {
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
    reduce.writeAsText(outFile, FileSystem.WriteMode.OVERWRITE);
  }
}
