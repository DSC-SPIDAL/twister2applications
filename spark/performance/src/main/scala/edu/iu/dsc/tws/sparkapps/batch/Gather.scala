package edu.iu.dsc.tws.sparkapps.batch

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pulasthi on 2/15/18.
  */
class  Gather(paralelizm: Int, size: Int,iterations: Int, outFile: String) {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_performance_gather")
    val sc = new SparkContext(conf)
    val gen = new Generator();
    val data = gen.getStringData(paralelizm, size, iterations)
    print("Length of data : " + data.length)

    val tempArray = 0 to (iterations) toArray;
    val parallelRDD = sc.parallelize(data,paralelizm);
    val results = parallelRDD.map(s => s).collect();
    print("The number of strings in results is : " + results.length)

  }

}
