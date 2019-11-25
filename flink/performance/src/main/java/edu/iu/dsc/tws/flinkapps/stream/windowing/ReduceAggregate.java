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
import org.apache.flink.streaming.api.windowing.time.Time;

public class ReduceAggregate {

    private int size;
    private int iterations;
    private int warmupIterations;
    private int windowLength;
    private int slidingWindowLength;
    private boolean isTimeWindow = false;
    private boolean isEventTime = false;
    private boolean isTimingEnabled = false;
    private String aggregationType;
    private long throttleTime = 100;
    private StreamExecutionEnvironment env;

    public ReduceAggregate(int size, int iterations, int warmupIterations, int windowLength, int slidingWindowLength,
                           boolean isTimeWindow, boolean isEventTime, boolean isTimingEnabled,
                           long throttleTime, StreamExecutionEnvironment env) {
        this.size = size;
        this.iterations = iterations;
        this.windowLength = windowLength;
        this.slidingWindowLength = slidingWindowLength;
        this.isTimeWindow = isTimeWindow;
        this.isTimingEnabled = isTimingEnabled;
        this.isEventTime = isEventTime;
        this.warmupIterations = warmupIterations;
        this.throttleTime = throttleTime;
        this.env = env;
    }

    public void execute() {
        DataStream<CollectiveData> stringStream = env
                .addSource(new RichParallelSourceFunction<CollectiveData>() {
                    int count = 0;
                    int size = 0;
                    int iterations = 10000;
                    int warmupIterations = 10000;
                    boolean isEventTime;
                    boolean isTimingEnabled;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ParameterTool p = (ParameterTool)
                                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        size = p.getInt("size", 128000);
                        iterations = p.getInt("itr", 10000);
                        warmupIterations = p.getInt("witr", 10000);
                        isEventTime = p.getBoolean("eventTime", false);
                        isTimingEnabled = p.getBoolean("enableTime", false);
                        //System.out.println("open: " + size + "," + iterations + "," + warmupIterations);
                    }

                    @Override
                    public void run(SourceContext<CollectiveData> sourceContext) throws Exception {
                        System.out.println("run: " + count + "/" + warmupIterations + "," + iterations);
                        while (count < iterations + warmupIterations) {
                            CollectiveData i = new CollectiveData(size, count);
                            if (isTimingEnabled) {
                                if (isEventTime) {

                                } else {

                                }
                            } else {
                                sourceContext.collect(i);
                            }
                            Thread.sleep(throttleTime);
                            //System.out.println("source: " + count);
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

        if (isTimeWindow) {
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
                    int warmupIterations;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ParameterTool p = (ParameterTool)
                                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        iterations = p.getInt("itr", 10000);
                        warmupIterations = p.getInt("witr", 10000);
                        //System.out.println("7777 iterations: " + iterations);
                    }

                    @Override
                    public void invoke(Tuple2<Integer, CollectiveData> integerStringTuple2, Context context) throws Exception {
                        if (count == 0) {
                            start = System.nanoTime();
                        }
                        CollectiveData c = integerStringTuple2.f1;
                        System.out.println(count + "/" + warmupIterations);
                        if (count > warmupIterations && c != null) {
                            long timeNow = System.nanoTime();
                            String hostInfo = GetInfo.hostInfo();
                            //System.out.println("I am invoked inside count not null: " + c.getSummary() + "," + hostInfo);

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
                            System.out.println("reduce," + count + ", " + c.getIteration() + ", " + timeNow + ","
                                    + c.getList().length);


                            //System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (integerStringTuple2.f1));
                        }
                        count++;
                    }

//                    @Override
//                    public void invoke(Tuple2<Integer, CollectiveData> integerStringTuple2) throws Exception {
//
//                    }
                });


    }

    public void saveLogs() {

    }
}
