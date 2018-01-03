package edu.iu.dsc.tws.flinkapps;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Program {
  public static void main(String[] args) throws Exception {
    // Checking input parameters
    final ParameterTool params = ParameterTool.fromArgs(args);

    int mode = params.getInt("stream", 1);
    if (mode == 0) {
      batch(params);
    } else if (mode == 1) {
      streaming(params);
    }
  }

  private static void batch(ParameterTool params) throws Exception {
    // set up execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);

    int size = params.getInt("size", 1);
    int itr = params.getInt("itr", 10);
    int col = params.getInt("col", 0);

    if (col == 0) {
      Reduce reduce = new Reduce(size, itr, env, "");
      reduce.execute();
      env.execute();
    } else if (col == 1) {
      Gather gather = new Gather(size, itr, env, "");
      gather.execute();
      env.execute();
    }
  }

  private static void streaming(ParameterTool params) throws Exception {
    // set up execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);

    int size = params.getInt("size", 1);
    int itr = params.getInt("itr", 10);
    int col = params.getInt("col", 0);

    StreamingReduce streamingReduce = new StreamingReduce(size, itr, env, "");
    streamingReduce.execute();
    env.execute();
  }
}
