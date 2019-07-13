package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ByteInputFormat extends InputFormat<byte[], byte[]> {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      splits.add(new ByteInputSplit());
    }
    return splits;
  }

  @Override
  public RecordReader<byte[], byte[]> createRecordReader(InputSplit inputSplit,
                                                         TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new ByteRecordReader();
  }
}
