package edu.iu.dsc.tws.sparkapps.streaming

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark._
import org.apache.spark.streaming._

class Gather(paralelizm: Int, size: Int, iterations: Int) {
  def execute(): Unit = {
    val conf = new SparkConf().setAppName("spark_performance_gather")
    val ssc = new StreamingContext(conf, Milliseconds(1))
    val gen = new Generator();
    val data = gen.getStringData(paralelizm, size, iterations)
    print("Length of data : " + data.length)

    val tempArray = 0 to (iterations) toArray;
    var startTime = System.currentTimeMillis();
    val parallelRDD = ssc.sparkContext.parallelize(data, paralelizm)
    val results = parallelRDD.map(s => s).collect();
    var endTime = System.currentTimeMillis();
    print("\n---------------------------------------------------")
    print("\nThe number of int[] in results is : " + results.length)
    print("\nTotal time for Gather"
      + "\n size : " + size
      + "\n iterations : " + iterations
      + "\n para : " + paralelizm
      + "\n Time : " + (endTime - startTime));
    print("\n---------------------------------------------------")
  }
}
