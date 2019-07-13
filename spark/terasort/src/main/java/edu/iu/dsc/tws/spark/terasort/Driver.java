package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class Driver {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);

    input.partitionBy(new Partitioner() {
      @Override
      public int getPartition(Object key) {
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

      private int keysToOneTask;

      private List<Integer> destinationsList;

      private boolean initialized = false;

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
        return integer;
      }

      @Override
      public int numPartitions() {
        return 10;
      }
    });


  }
}
