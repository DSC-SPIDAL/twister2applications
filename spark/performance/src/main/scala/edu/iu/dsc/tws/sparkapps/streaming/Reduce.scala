package edu.iu.dsc.tws.sparkapps.streaming

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class Reduce(paralelizm: Int, size: Int, iterations: Int)  {
  def execute(): Unit = {
    val conf = new SparkConf().setAppName("spark_performance_reduce")
    conf.set("spark.dynamicAllocation.enabled", "false");
    val ssc = new StreamingContext(conf, Seconds(1))
    val gen = new Generator();
    val data = gen.getIntData(paralelizm, size, iterations)
    print("Length of data : " + data.length)
    val startTime = System.currentTimeMillis()
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => x)
    val reducedStream = mappedStream.reduce((s1,s2) => s1 + s2)
    reducedStream.print()
    ssc.start()
    for (i <- 1 to iterations) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to size, paralelizm)
      }
      Thread.sleep(5)
    }
    ssc.stop()
//
//    val tempArray = 0 to (iterations) toArray;
//    var startTime = System.currentTimeMillis();
//    val parallelRDD = ssc
//      .sparkContext
//      .parallelize( data, paralelizm)
//    val maps = parallelRDD
//        .map(s=>s)
//    val results = maps.fold(0)(_+_)



    var endTime = System.currentTimeMillis();
    print("\n---------------------------------------------------")
    print("\nThe number of int[] in results is : " + reducedStream)
    print("\nTotal time for Reduce"
      + "\n size : " + size
      + "\n iterations : " + iterations
      + "\n para : " + paralelizm
      + "\n Time : " + (endTime - startTime));
    print("\n---------------------------------------------------\n")
  }
}
