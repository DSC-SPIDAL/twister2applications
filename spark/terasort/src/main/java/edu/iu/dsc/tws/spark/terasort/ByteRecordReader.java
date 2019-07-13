package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Random;

public class ByteRecordReader extends RecordReader<byte[], byte[]> {
  private int numRecords = 10000;

  private int currentRead = 0;

  private Random random;

  public ByteRecordReader() {
    random = new Random(System.nanoTime());
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return currentRead++ < numRecords;
  }

  @Override
  public byte[] getCurrentKey() throws IOException, InterruptedException {
    byte[] key = new byte[10];
    random.nextBytes(key);
    return key;
  }

  @Override
  public byte[] getCurrentValue() throws IOException, InterruptedException {
    byte[] key = new byte[90];
    random.nextBytes(key);
    return key;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentRead / numRecords;
  }

  @Override
  public void close() throws IOException {

  }
}
