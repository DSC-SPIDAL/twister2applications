package edu.iu.dsc.tws.flinkapps.batch;

import edu.iu.dsc.tws.flinkapps.data.ByteArrayComparator;
import edu.iu.dsc.tws.flinkapps.data.ByteInputFormat;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class TeraSort {
  private static final Logger LOG = LoggerFactory.getLogger(TeraSort.class);

  private int size;

  private int iterations;

  private ExecutionEnvironment env;

  private String outFile;

  public TeraSort(int size, int iterations, ExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public static class Part implements Partitioner<byte[]> {
    private int keysToOneTask;

    private List<Integer> destinationsList;

    private boolean initialized = false;

    @Override
    public int partition(byte[] bytes, int i) {
      if (!initialized) {
        Set<Integer> h = new HashSet<>();
        for (int j = 0; j < i; j++) {
          h.add(j);
        }
        prepare(h);
        initialized = true;
      }

      return partition(bytes);
    }

    void prepare(Set<Integer> destinations) {
      int totalPossibilities = 256 * 256; //considering only most significant bytes of array
      this.keysToOneTask = (int) Math.ceil(totalPossibilities / (double) destinations.size());
      this.destinationsList = new ArrayList<>(destinations);
      Collections.sort(this.destinationsList);
    }

    int getIndex(byte[] array) {
      int key = ((array[0] & 0xff) << 8) + (array[1] & 0xff);
      return key / keysToOneTask;
    }

    int partition(byte[] data) {
      Integer integer = this.destinationsList.get(this.getIndex(data));
      LOG.info("Returning: " + integer);
      return integer;
    }
  }

  public static class Out implements OutputFormat<Tuple2<byte[], byte[]>>, Serializable {
    byte[] previousKey = null;
    BytePrimitiveArrayComparator bytePrimitiveArrayComparator;

    @Override
    public void configure(Configuration configuration) {
      bytePrimitiveArrayComparator = new BytePrimitiveArrayComparator(true);
    }

    @Override
    public void open(int i, int i1) throws IOException {
    }

    @Override
    public void writeRecord(Tuple2<byte[], byte[]> tuple2) throws IOException {
      if (previousKey != null
          && bytePrimitiveArrayComparator.compare(previousKey, tuple2.f0) > 0) {
        System.out.println("Un-ordered");
      }
      previousKey = tuple2.f0;
    }

    @Override
    public void close() throws IOException {
    }
  }

  public void execute() throws Exception {
    DataSource<Tuple2<byte[], byte[]>> source = env.createInput(new ByteInputFormat());
    PartitionOperator<Tuple2<byte[], byte[]>> part = source.partitionCustom(new Part(), 0);
    TypeInformation<byte[]> info = TypeInformation.of(new TypeHint<byte[]>(){});

    part.sortPartition(0, Order.ASCENDING).output(new Out());
  }
}
