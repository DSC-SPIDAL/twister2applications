package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.common.io.GenericInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IntegerListFormat extends GenericInputFormat<List<Integer>> {
  int count = 0;

  int size = 0;

  public IntegerListFormat(int size) {
    this.size = size;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return count >= 1;
  }

  @Override
  public List<Integer> nextRecord(List<Integer> integers) throws IOException {
    count++;

    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(i);
    }

    return list;
  }
}
