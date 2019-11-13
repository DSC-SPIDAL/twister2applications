package edu.iu.dsc.tws.flinkapps.stream.windowing;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowAggregate {

    private static int size;
    private static int iterations;
    private static int warmupIterations;
    private static int windowLength;
    private static int slidingWindowLength;
    private static String aggregationType;
    private static boolean isTime = false;
    private static long throttleTime = 100;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        size = params.getInt("size", 256);
        iterations = params.getInt("itr", 10000);
        warmupIterations = params.getInt("witr", 10000);
        aggregationType = params.get("agg");
        windowLength = params.getInt("windowLength", 5);
        slidingWindowLength = params.getInt("slidingLength", 3);
        isTime = params.getBoolean("time", false);
        throttleTime = params.getLong("-throttleTime", 100);

        if (aggregationType.equals("gather")) {
            GatherAggregate gatherAggregate = new GatherAggregate(size, iterations, warmupIterations, windowLength,
                    slidingWindowLength, isTime, throttleTime, env);
            gatherAggregate.execute();
        }

        if (aggregationType.equals("reduce")) {
            ReduceAggregate reduceAggregate = new ReduceAggregate(size, iterations, warmupIterations, windowLength,
                    slidingWindowLength, isTime, throttleTime, env);
            reduceAggregate.execute();
        }
        //execute flink job
        env.execute();


    }
}
