package edu.iu.dsc.tws.flinkapps.stream;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class StreamPartitioning {
  int size;
  int iterations;
  StreamExecutionEnvironment env;
  String outFile;

  public StreamPartitioning(int size, int iterations, StreamExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public void execute() {
    DataStream<Tuple2<Integer, CollectiveData>> stringStream = env.addSource(new RichParallelSourceFunction<CollectiveData>() {
      int i = 1;
      int count = 0;
      int size = 0;
      int iterations = 10000;

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool p = (ParameterTool)
            getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        size = p.getInt("size", 128000);
        iterations = p.getInt("itr", 10000);
        System.out.println("iterations: " + iterations + " size: " + size);
      }

      @Override
      public void run(SourceContext<CollectiveData> sourceContext) throws Exception {
        while (count < iterations) {
          CollectiveData i = new CollectiveData(size);
          sourceContext.collect(i);
          count++;
        }
      }

      @Override
      public void cancel() {
      }
    }).map(new RichMapFunction<CollectiveData, Tuple2<Integer, CollectiveData>>() {
      Random random;

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
      }

      @Override
      public Tuple2<Integer, CollectiveData> map(CollectiveData s) throws Exception {
        return new Tuple2<>(random.nextInt(640 * 2), s);
      }
    }).partitionCustom(new MyPartitioner(), 0);

    stringStream.addSink(new RichSinkFunction<Tuple2<Integer, CollectiveData>>() {
      @Override
      public void invoke(Tuple2<Integer, CollectiveData> integerCollectiveDataTuple2) throws Exception {
        if (count == 0) {
          start = System.nanoTime();
        }
        count++;
//        if (count >= iterations) {
//          System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " ");
//        }
      }
      long start;
      int count = 0;
      int iterations;
    });
  }

  public class MyPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer key, int numPartitions) {
      return key % numPartitions;
    }
  }
}
