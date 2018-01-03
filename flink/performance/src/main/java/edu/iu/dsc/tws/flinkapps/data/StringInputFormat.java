package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.common.io.GenericInputFormat;

import java.io.IOException;
import java.util.Random;

public class StringInputFormat extends GenericInputFormat<String> {
  private int count = 0;

  private int elements;

  private RandomString randomString;

  public StringInputFormat(int size, int elements) {
    this.elements = elements;
    this.randomString = new RandomString(size, new Random(System.nanoTime()), RandomString.alphanum);
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return count >= elements;
  }

  @Override
  public String nextRecord(String s) throws IOException {
    count++;
    return randomString.nextString();
  }
}
