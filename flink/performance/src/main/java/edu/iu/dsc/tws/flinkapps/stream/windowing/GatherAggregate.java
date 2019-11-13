package edu.iu.dsc.tws.flinkapps.stream.windowing;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;
import edu.iu.dsc.tws.flinkapps.util.GetInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class GatherAggregate {

    private int size;
    private int iterations;
    private int warmupIterations;
    private int windowLength;
    private int slidingWindowLength;
    private boolean isTime = false;
    private String aggregationType;
    private StreamExecutionEnvironment env;

    public GatherAggregate(int size, int iterations, int warmupIterations, int windowLength, int slidingWindowLength,
                           boolean isTime, StreamExecutionEnvironment env) {
        this.size = size;
        this.iterations = iterations;
        this.windowLength = windowLength;
        this.slidingWindowLength = slidingWindowLength;
        this.isTime = isTime;
        this.warmupIterations = warmupIterations;
        this.env = env;
    }

    public void execute() {
        DataStream<CollectiveData> stringStream = env
                .addSource(new RichParallelSourceFunction<CollectiveData>() {
                    int count = 0;
                    int size = 0;
                    int iterations = 10000;
                    int warmupIterations;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ParameterTool p = (ParameterTool)
                                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        size = p.getInt("size", 128000);
                        iterations = p.getInt("itr", 10000);
                        warmupIterations = p.getInt("witr", 10000);
                    }

                    @Override
                    public void run(SourceContext<CollectiveData> sourceContext) throws Exception {
                        while (count < iterations + warmupIterations) {
                            CollectiveData i = new CollectiveData(size, count);
                            sourceContext.collect(i);
                            //System.out.println("gather-source," + count);
                            //System.out.println(i.getSummary());
                            count++;
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                });

        SingleOutputStreamOperator<Tuple2<Integer, CollectiveData>> mapData = stringStream.map(new RichMapFunction<CollectiveData, Tuple2<Integer, CollectiveData>>() {
            @Override
            public Tuple2<Integer, CollectiveData> map(CollectiveData s) throws Exception {
                return new Tuple2<Integer, CollectiveData>(0, s);
            }
        });

        KeyedStream<Tuple2<Integer, CollectiveData>, Tuple> keyedStream = mapData.keyBy(0);

        WindowedStream<Tuple2<Integer, CollectiveData>, Tuple, ?> windowedStream = null;

        if (isTime) {
            // time windows
            if (slidingWindowLength == 0) {
                // tumbling window
                windowedStream = keyedStream.timeWindow(Time.milliseconds(windowLength));
            } else {
                // sliding window
                windowedStream = keyedStream.timeWindow(Time.milliseconds(windowLength),
                        Time.milliseconds(slidingWindowLength));
            }


        } else {
            // count windows
            if (slidingWindowLength == 0) {
                // tumbling window
                windowedStream = keyedStream.countWindow(windowLength);
            } else {
                // sliding window
                windowedStream = keyedStream.countWindow(windowLength, slidingWindowLength);
            }
        }

        SingleOutputStreamOperator<List<CollectiveData>> aggregateWindowedStream = windowedStream
                .aggregate(new AggregateFunction<Tuple2<Integer, CollectiveData>, List<CollectiveData>, List<CollectiveData>>() {

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
                });
        DataStreamSink<List<CollectiveData>> dataStreamSink = aggregateWindowedStream
                .addSink(new RichSinkFunction<List<CollectiveData>>() {

                    long start;
                    int count = 0;
                    int iterations;
                    int warmupIterations;

                    @Override
                    public void invoke(List<CollectiveData> value, Context context) throws Exception {
                        if (count == 0) {
                            start = System.nanoTime();
                        }
                        //System.out.println("within invoke");
                        CollectiveData c = value.get(0);
                        if (count > warmupIterations && c != null) {

                            long timeNow = System.nanoTime();
                            String hostInfo = GetInfo.hostInfo();

//                        System.out.println("sink,"
//                                + count + ","
//                                + c.getIteration() + ","
//                                + c.getIterationString() + ","
//                                + c.getMessageTime() + ","
//                                + timeNow
//                                + "," + (timeNow - c.getMessageTime()) / 1000000.0 + ","
//                                + hostInfo + ","
//                                + c.getMeta() + ","
//                                + c.getList().length);
                            System.out.println("gather," + count + ", " + c.getIteration() + ", " + timeNow + "," + c.getList().length);


                            //System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (integerStringTuple2.f1));
                        }
                        count++;
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ParameterTool p = (ParameterTool)
                                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        iterations = p.getInt("itr", 10000);
                        warmupIterations = p.getInt("witr", 10000);
                        //System.out.println("7777 iterations: " + iterations);
                    }
                });


    }
}
