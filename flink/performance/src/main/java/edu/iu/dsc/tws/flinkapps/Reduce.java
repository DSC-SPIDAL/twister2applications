package edu.iu.dsc.tws.flinkapps;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Reduce {
  private int size;
  private int iterations;
  private StreamExecutionEnvironment env;
  private String outFile;

  public Reduce(int size, int iterations, StreamExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }
}
