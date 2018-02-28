package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CollectionInputFormat;

import java.util.ArrayList;
import java.util.List;

public final class Generator {
  public static DataSet<String> generateStringSet(ExecutionEnvironment env, int length, int counts) {
    return env.createInput(new StringInputFormat(length, counts));
  }

  public static DataSet<Integer> generateOneElementDataSet(ExecutionEnvironment env) {
    return env.createInput(new IntegerInputFormat());
  }

  public static DataSet<CollectiveData> loadDataSet(int size, ExecutionEnvironment env) {
    return env.createInput(new CollectiveDataFormat(size));
  }

  public static DataSet<Integer> loadMapDataSet(int size, ExecutionEnvironment env) {
    int p =  env.getParallelism();
    List<Integer> list = new ArrayList<>();
    for (int j = 0; j < p; j++) {
      list.add(j);
    }
    return env.fromCollection(list);
  }
}


