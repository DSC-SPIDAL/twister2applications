package edu.iu.dsc.tws.flinkapps.stream;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

public class StreamingGather {
  int size;
  int iterations;
  StreamExecutionEnvironment env;
  String outFile;

  public StreamingGather(int size, int iterations, StreamExecutionEnvironment env, String outFile) {
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
    }).keyBy(0).countWindow(1).aggregate(new AggregateFunction<Tuple2<Integer,CollectiveData>, List<CollectiveData>, List<CollectiveData>>() {
      @Override
      public List<CollectiveData> createAccumulator() {
        return new ArrayList<>();
      }

      @Override
      public List<CollectiveData> add(Tuple2<Integer, CollectiveData> integerCollectiveDataTuple2, List<CollectiveData> collectiveData) {
        collectiveData.add(integerCollectiveDataTuple2.f1);
        return collectiveData;
      }


      @Override
      public List<CollectiveData> getResult(List<CollectiveData> collectiveData) {
        return collectiveData;
      }

      @Override
      public List<CollectiveData> merge(List<CollectiveData> collectiveData, List<CollectiveData> acc1) {
        List<CollectiveData> d = new ArrayList<>();
        d.addAll(collectiveData);
        d.addAll(acc1);
        return d;
      }
    }).addSink(new RichSinkFunction<List<CollectiveData>>() {
      @Override
      public void invoke(List<CollectiveData> collectiveData) throws Exception {
        if (count == 0) {
          start = System.nanoTime();
        }
        count++;
        if (count >= iterations) {
          System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (collectiveData.size()));
        }
      }

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
    });

  }

  private static CollectiveData add(CollectiveData i, CollectiveData j) {
    int[] r= new int[i.getList().length];
    for (int k = 0; k < i.getList().length; k++) {
      r[k] = ((i.getList()[k] + j.getList()[k]));
    }
    return new CollectiveData(r);
  }
}
