package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ByteInputFormat extends InputFormat<byte[], byte[]> {

  private int parallel = 10;

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < parallel; i++) {
      ByteInputSplit e = new ByteInputSplit();
      String node = "v-0";
      int index = i % 16;
      if (index >= 10) {
        node += index;
      } else {
        node += "0" + index;
      }
      e.setNode(node);
      splits.add(e);
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
