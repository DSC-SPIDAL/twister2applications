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

import java.util.List;

public class GatherAggregate {

    private int size;
    private int iterations;
    private int windowLength;
    private int slidingWindowLength;
    private boolean isTime = false;
    private String aggregationType;
    private StreamExecutionEnvironment env;

    public GatherAggregate(int size, int iterations, int windowLength, int slidingWindowLength, boolean isTime,
                           StreamExecutionEnvironment env) {
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
                        return null;
                    }

                    @Override
                    public List<CollectiveData> add(Tuple2<Integer, CollectiveData> integerCollectiveDataTuple2, List<CollectiveData> collectiveData) {
                        return null;
                    }

                    @Override
                    public List<CollectiveData> getResult(List<CollectiveData> collectiveData) {
                        return null;
                    }

                    @Override
                    public List<CollectiveData> merge(List<CollectiveData> collectiveData, List<CollectiveData> acc1) {
                        return null;
                    }
                });
        DataStreamSink<List<CollectiveData>> dataStreamSink = aggregateWindowedStream
                .addSink(new RichSinkFunction<List<CollectiveData>>() {

                    long start;
                    int count = 0;
                    int iterations;

                    @Override
                    public void invoke(List<CollectiveData> value, Context context) throws Exception {
                        if (count == 0) {
                            start = System.nanoTime();
                        }
                        count++;


                        if (count >= iterations) {
                            CollectiveData c = value.get(0);
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
                            System.out.println(count + ", " + c.getIteration() + ", " + timeNow + "," + c.getList().length);


                            //System.out.println("Final: " + count + " " + (System.nanoTime() - start) / 1000000 + " " + (integerStringTuple2.f1));
                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ParameterTool p = (ParameterTool)
                                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                        iterations = p.getInt("itr", 10000);
                        //System.out.println("7777 iterations: " + iterations);
                    }
                });


    }
}
