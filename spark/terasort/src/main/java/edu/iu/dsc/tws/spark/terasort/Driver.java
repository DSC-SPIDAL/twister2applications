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

    JavaPairRDD<byte[], byte[]> partition = input.partitionBy(new TeraSortPartitioner());

    JavaPairRDD<byte[], byte[]> sorted = partition.sortByKey(new ByteComparator());


    sorted.saveAsTextFile("out");
  }
}
