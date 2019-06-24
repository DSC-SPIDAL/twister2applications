package edu.iu.dsc.tws.flinkapps.data;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Random;

public class ByteInputFormat extends GenericInputFormat<Tuple2<byte[], byte[]>> {
  private int count = 0;

  private int numTuples = 10;

  private Random random;

  private int keySize = 10;

  private int valueSize = 90;

  public ByteInputFormat() {
  }

  public ByteInputFormat(int numTuples) {
    this.numTuples = numTuples;
  }

  @Override
  public void configure(Configuration parameters) {
    super.configure(parameters);
    keySize = parameters.getInteger("keySize", 10);
    valueSize = parameters.getInteger("valueSize", 90);
    random = new Random(System.nanoTime());
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return count >= numTuples;
  }

  @Override
  public Tuple2<byte[], byte[]> nextRecord(Tuple2<byte[], byte[]> tuple2) throws IOException {
    byte[] key = new byte[keySize];
    byte[] val = new byte[valueSize];
    random.nextBytes(key);
    random.nextBytes(val);
    count++;
    return new Tuple2<>(key, val);
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public int getNumTuples() {
    return numTuples;
  }

  public void setNumTuples(int numTuples) {
    this.numTuples = numTuples;
  }

  public int getKeySize() {
    return keySize;
  }

  public void setKeySize(int keySize) {
    this.keySize = keySize;
  }

  public int getValueSize() {
    return valueSize;
  }

  public void setValueSize(int valueSize) {
    this.valueSize = valueSize;
  }
}
