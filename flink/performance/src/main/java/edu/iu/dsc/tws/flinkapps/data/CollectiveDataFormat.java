package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.common.io.GenericInputFormat;

import java.io.IOException;

public class CollectiveDataFormat extends GenericInputFormat<CollectiveData> {
  int count = 0;

  int size = 0;

  public CollectiveDataFormat(int size) {
    this.size = size;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return count >= 0;
  }

  @Override
  public CollectiveData nextRecord(CollectiveData collectiveData) throws IOException {
    return null;
  }
}
