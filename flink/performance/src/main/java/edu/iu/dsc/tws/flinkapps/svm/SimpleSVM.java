package edu.iu.dsc.tws.flinkapps.svm;


import edu.iu.dsc.tws.flinkapps.data.CollectiveData;
import edu.iu.dsc.tws.flinkapps.data.CollectiveDoubleData;
import edu.iu.dsc.tws.flinkapps.stream.StreamingReduce;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class SimpleSVM {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        int parallelism = params.getInt("parallelism");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        long startTime = System.currentTimeMillis();
        SVMReduce streamingReduce = new SVMReduce(env);
        streamingReduce.execute();
        env.execute();
        long endTime = System.currentTimeMillis();
        System.out.println("End Time : " + (endTime - startTime) / 1000.0);

    }




}
