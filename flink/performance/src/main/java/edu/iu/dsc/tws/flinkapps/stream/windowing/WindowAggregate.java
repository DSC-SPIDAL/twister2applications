package edu.iu.dsc.tws.flinkapps.stream.windowing;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowAggregate {

    private static int size;
    private static int iterations;
    private static int windowLength;
    private static int slidingWindowLength;
    private static String aggregationType;
    private static boolean isTime = false;

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        size = params.getInt("size", 256);
        iterations = params.getInt("itr", 10000);
        aggregationType = params.get("agg");
        windowLength = params.getInt("windowLength", 5);
        slidingWindowLength = params.getInt("slidingLength", 3);
        isTime = params.getBoolean("time", false);

        if (aggregationType.equals("gather")) {

        }

        if (aggregationType.equals("reduce")) {
            ReduceAggregate reduceAggregate = new ReduceAggregate(size, iterations, windowLength,
            slidingWindowLength, isTime, env);
            reduceAggregate.execute();
        }


    }
}
