package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public final class Generator {
  public static DataSet<String> generateStringSet(ExecutionEnvironment env, int length, int counts) {
    return env.createInput(new StringInputFormat(length, counts));
  }
}
