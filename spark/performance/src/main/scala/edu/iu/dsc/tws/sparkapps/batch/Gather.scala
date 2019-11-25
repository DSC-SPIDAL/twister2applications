package edu.iu.dsc.tws.sparkapps.batch

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark.{SparkConf, SparkContext}


class Gather(paralelizm: Int, size: Int, iterations: Int) {
  def execute(): Unit = {
    val conf = new SparkConf().setAppName("spark_performance_gather")
    val sc = new SparkContext(conf)
    val gen = new Generator();
    val data = gen.getStringData(paralelizm, size, iterations)
    print("Length of data : " + data.length)

    val tempArray = 0 to (iterations) toArray;
    var startTime = System.currentTimeMillis();

    val parallelRDD = sc.parallelize(data, paralelizm);
    val results = parallelRDD.map(s => s).collect();
    results.foreach(p=> print(p))
    var endTime = System.currentTimeMillis();

    print("The number of strings in results is : " + results.length)
    print("Total time for Gather"
      + " size : " + size
      + " iterations : " + iterations
      + " para : " + paralelizm
      + " Time : " + (endTime - startTime)
    )
  }
}
