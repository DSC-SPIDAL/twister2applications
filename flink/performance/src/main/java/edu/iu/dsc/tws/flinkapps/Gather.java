package edu.iu.dsc.tws.flinkapps;

import edu.iu.dsc.tws.flinkapps.data.Generator;
import edu.iu.dsc.tws.flinkapps.data.StringInputFormat;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class Gather {
  private int size;
  private int iterations;
  private ExecutionEnvironment env;
  private String outFile;

  public Gather(int size, int iterations, ExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public void execute() {
    DataSet<String> stringStream = Generator.generateStringSet(env, size, iterations);

    DataSet<String> reduce = stringStream.map(new RichMapFunction<String, Tuple2<Integer, String>>() {
      int pid;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        pid = getRuntimeContext().getIndexOfThisSubtask();
      }

      @Override
      public Tuple2<Integer, String> map(String s) throws Exception {
        return new Tuple2<Integer, String>(0, s);
      }
    }).groupBy(0).reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, String>, String>() {
      @Override
      public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<String> collector) throws Exception {
        String s = "";
        for (Tuple2<Integer, String> e : iterable) {
          s += e.f1;
        }
        collector.collect(s);
      }
    });

    reduce.writeAsText("out.txt", FileSystem.WriteMode.OVERWRITE);
  }
}

