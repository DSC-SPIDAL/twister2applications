package edu.iu.dsc.tws.flinkapps;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Utils {
  public static DataSet<CollectiveData> loadDataSet(int size, ExecutionEnvironment env) {
    int p =  env.getParallelism();
    List<Integer> data = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      data.add(i);
    }

//    CollectiveData collectiveData = new CollectiveData(data);
//    return env.fromElements(collectiveData);
    return null;
  }
}
