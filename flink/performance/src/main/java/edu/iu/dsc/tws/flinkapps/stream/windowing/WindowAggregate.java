package edu.iu.dsc.tws.flinkapps.stream.windowing;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowAggregate {


    private static int size;
    private static int iterations;
    private static int warmupIterations;
    private static int windowLength;
    private static int slidingWindowLength;
    private static String aggregationType;
    private static boolean isTimeWindow = false;
    private static boolean isTimingEnabled = false; //decide whether to use process/event timing or use none of process/event timing
    private static long throttleTime = 100;
    private static boolean isEventTime = false;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        size = params.getInt("size", 256);
        iterations = params.getInt("itr", 10000);
        warmupIterations = params.getInt("witr", 10000);
        aggregationType = params.get("agg");
        windowLength = params.getInt("windowLength", 5);
        slidingWindowLength = params.getInt("slidingLength", 3);
        isTimeWindow = params.getBoolean("time", false);
        isEventTime = params.getBoolean("eventTime", false);
        isTimingEnabled = params.getBoolean("enableTime", false);
        throttleTime = params.getLong("throttleTime", 100);

        if (isEventTime) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        }

        if (aggregationType.equals("gather")) {
            GatherAggregate gatherAggregate = new GatherAggregate(size, iterations, warmupIterations, windowLength,
                    slidingWindowLength, isTimeWindow, isEventTime, isTimingEnabled, throttleTime, env);
            gatherAggregate.execute();
        }

        if (aggregationType.equals("reduce")) {
            ReduceAggregate reduceAggregate = new ReduceAggregate(size, iterations, warmupIterations, windowLength,
                    slidingWindowLength, isTimeWindow, isEventTime, isTimingEnabled, throttleTime, env);
            reduceAggregate.execute();
        }
        //execute flink job
        env.execute();


    }
}
