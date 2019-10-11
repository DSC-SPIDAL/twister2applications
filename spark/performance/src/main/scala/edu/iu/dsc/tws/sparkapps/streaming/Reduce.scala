package edu.iu.dsc.tws.sparkapps.streaming

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

class Reduce(paralelizm: Int, size: Int, iterations: Int)  {
  def execute(): Unit = {
    val conf = new SparkConf().setAppName("spark_performance_reduce")
    val ssc = new StreamingContext(conf, Milliseconds(1))
    val gen = new Generator();
    val data = gen.getIntData(paralelizm, size, iterations)
    print("Length of data : " + data.length)

    val tempArray = 0 to (iterations) toArray;
    var startTime = System.currentTimeMillis();
    val parallelRDD = ssc
      .sparkContext
      .parallelize( 1 to iterations * paralelizm * size, paralelizm);
    val results = parallelRDD.reduce((s1,s2) => s1 + s2)
    var endTime = System.currentTimeMillis();
    print("\n---------------------------------------------------")
    print("\nThe number of int[] in results is : " + results)
    print("\nTotal time for Reduce"
      + "\n size : " + size
      + "\n iterations : " + iterations
      + "\n para : " + paralelizm
      + "\n Time : " + (endTime - startTime));
    print("\n---------------------------------------------------\n")
  }
}
