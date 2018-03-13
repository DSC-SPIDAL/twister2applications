package edu.iu.dsc.tws.flinkapps.stream;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

public class StreamingReduce {
  int size;
  int iterations;
  StreamExecutionEnvironment env;
  String outFile;

  public StreamingReduce(int size, int iterations, StreamExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public void execute() {
    DataStream<CollectiveData> stringStream = env.addSource(new RichParallelSourceFunction<CollectiveData>() {
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
        System.out.println("6666 iterations: " + iterations + " size: " + size);
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
    });

    stringStream.map(new RichMapFunction<CollectiveData, Tuple2<Integer, CollectiveData>>() {
      @Override
      public Tuple2<Integer, CollectiveData> map(CollectiveData s) throws Exception {
        return new Tuple2<Integer, CollectiveData>(0, s);
      }
    }).keyBy(0).countWindow(1).reduce(new ReduceFunction<Tuple2<Integer, CollectiveData>>() {
      @Override
      public Tuple2<Integer, CollectiveData> reduce(Tuple2<Integer, CollectiveData> c1,
                                                    Tuple2<Integer, CollectiveData> c2) throws Exception {
        return new Tuple2<Integer, CollectiveData>(0, add(c1.f1, c2.f1));
      }
    }).addSink(new RichSinkFunction<Tuple2<Integer,CollectiveData>>() {
      long start;
      int count = 0;
      int iterations;

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool p = (ParameterTool)
            getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        iterations = p.getInt("itr", 10000);
        System.out.println("7777 iterations: " + iterations);
      }

      @Override
      public void invoke(Tuple2<Integer, CollectiveData> integerStringTuple2) throws Exception {
        if (count == 0) {
          start = System.nanoTime();
        }
        count++;
        if (count >= iterations) {
          System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (integerStringTuple2.f1));
        }
      }
    });

  }

  private static CollectiveData add(CollectiveData i, CollectiveData j) {
    List<Integer> r= new ArrayList<>();
    for (int k = 0; k < i.getList().size(); k++) {
      r.add((i.getList().get(k) + j.getList().get(k)));
    }
    return new CollectiveData(r);
  }
}
