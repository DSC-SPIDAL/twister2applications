package edu.iu.dsc.tws.flinkapps.stream.windowing;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;
import edu.iu.dsc.tws.flinkapps.util.GetInfo;
import edu.iu.dsc.tws.flinkapps.util.UtilCollective;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class ReduceAggregate {

    private int size;
    private int iterations;
    private int windowLength;
    private int slidingWindowLength;
    private boolean isTime = false;
    private String aggregationType;
    private StreamExecutionEnvironment env;

    public ReduceAggregate(int size, int iterations, int windowLength, int slidingWindowLength,
                           boolean isTime, StreamExecutionEnvironment env) {
        this.size = size;
        this.iterations = iterations;
        this.windowLength = windowLength;
        this.slidingWindowLength = slidingWindowLength;
        this.isTime = isTime;
        this.env = env;
    }

    public void execute() {
        DataStream<CollectiveData> stringStream = env
                .addSource(new RichParallelSourceFunction<CollectiveData>() {
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
                    }

                    @Override
                    public void run(SourceContext<CollectiveData> sourceContext) throws Exception {
                        while (count < iterations) {
                            CollectiveData i = new CollectiveData(size, count);
                            sourceContext.collect(i);
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

        WindowedStream<Tuple2<Integer, CollectiveData>, Tuple, GlobalWindow> windowedStream = null;

        if (slidingWindowLength == 0) {
            // tumbling window
        } else {
           // sliding window
        }

        windowedStream = keyedStream.countWindow(1);

        SingleOutputStreamOperator<Tuple2<Integer, CollectiveData>> reducedWindowStream = windowedStream
                .reduce(new ReduceFunction<Tuple2<Integer, CollectiveData>>() {
                    @Override
                    public Tuple2<Integer, CollectiveData> reduce(Tuple2<Integer, CollectiveData> c1,
                                                                  Tuple2<Integer, CollectiveData> c2) throws Exception {
                        return new Tuple2<Integer, CollectiveData>(0, UtilCollective.add(c1.f1, c2.f1));
                    }
                });

        DataStreamSink<Tuple2<Integer, CollectiveData>> dataStreamSink = reducedWindowStream
                .addSink(new RichSinkFunction<Tuple2<Integer, CollectiveData>>() {
                    long start;
                    int count = 0;
                    int iterations;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ParameterTool p = (ParameterTool)
                                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        iterations = p.getInt("itr", 10000);
                        //System.out.println("7777 iterations: " + iterations);
                    }

                    @Override
                    public void invoke(Tuple2<Integer, CollectiveData> integerStringTuple2) throws Exception {
                        if (count == 0) {
                            start = System.nanoTime();
                        }
                        count++;
                        CollectiveData c = integerStringTuple2.f1;
                        if (count >= iterations && c != null) {

                            long timeNow = System.nanoTime();
                            String hostInfo = GetInfo.hostInfo();
                            if (c.getMeta().equals(hostInfo)) {
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
                                System.out.println(count + ", " + c.getIteration() + ", " + timeNow + "," + c.getList().length);
                            }

                            //System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (integerStringTuple2.f1));
                        }
                    }
                });


    }

    public void saveLogs() {

    }
}
