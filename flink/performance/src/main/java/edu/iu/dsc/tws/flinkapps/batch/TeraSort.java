package edu.iu.dsc.tws.flinkapps.batch;

import edu.iu.dsc.tws.flinkapps.data.Generator;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class TeraSort {
  private int size;

  private int iterations;

  private ExecutionEnvironment env;

  private String outFile;

  public TeraSort(int size, int iterations, ExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public void execute() {
    DataSet<String> stringStream = Generator.generateStringSet(env, size, iterations);
    stringStream.flatMap(new RichFlatMapFunction<String, Tuple2<byte[], byte[]>>() {
      @Override
      public void flatMap(String s, Collector<Tuple2<byte[], byte[]>> collector) throws Exception {
        return;
      }
    }).partitionCustom(new Partitioner<Tuple2<byte[], byte[]>>() {
      @Override
      public int partition(Tuple2<byte[], byte[]> tuple2, int i) {
        return 0;
      }
    }, 0).write(new FileOutputFormat<Tuple2<byte[], byte[]>>() {
      @Override
      public void writeRecord(Tuple2<byte[], byte[]> tuple2) throws IOException {

      }
    }, "/tmp");
  }
}
