package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.common.io.GenericInputFormat;

import java.io.IOException;

public class IntegerInputFormat extends GenericInputFormat<Integer> {
  private int count = 0;

  @Override
  public boolean reachedEnd() throws IOException {
    return count >= 1;
  }

  @Override
  public Integer nextRecord(Integer integer) throws IOException {
    count++;
    return partitionNumber;
  }
}
