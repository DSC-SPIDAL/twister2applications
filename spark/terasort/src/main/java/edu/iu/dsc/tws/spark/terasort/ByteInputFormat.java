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
      splits.add(new InputSplit() {
        @Override
        public long getLength() throws IOException, InterruptedException {
          return 100 * 10000;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
          return new String[0];
        }
      });
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
