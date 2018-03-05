package edu.iu.dsc.tws.sparkapps.batch

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pulasthi on 2/15/18.
  */
class Reduce(paralelizm: Int, size: Int, iterations: Int) {
  def execute(): Unit = {
    val conf = new SparkConf().setAppName("spark_performance_reduce")
    val sc = new SparkContext(conf)
    val gen = new Generator();
    val data = gen.getStringData(paralelizm, size, iterations)
    print("Length of data : " + data.length)

    val tempArray = 0 to (iterations) toArray;
    val parallelRDD = sc.parallelize(data, paralelizm);
    var startTime = System.currentTimeMillis();
    val results = parallelRDD.map(s => s).reduce((s1, s2) => s1)
    var endTime = System.currentTimeMillis();

    print("The number of strings in results is : " + results.length)
    print("Total time for Reduce"
      + " size : " + size
      + " iterations : " + iterations
      + " para : " + paralelizm
      + " Time : " + (endTime - startTime));

  }
}
