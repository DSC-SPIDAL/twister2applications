package edu.iu.dsc.tws.spark.terasort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Driver {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    JavaSparkContext sc = new JavaSparkContext(conf);
  }
}
