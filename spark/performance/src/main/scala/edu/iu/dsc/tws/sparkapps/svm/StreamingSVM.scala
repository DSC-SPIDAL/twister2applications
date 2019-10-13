package edu.iu.dsc.tws.sparkapps.svm

import edu.iu.dsc.tws.sparkapps.data.Generator
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

class StreamingSVM(size: Int, paralelizm: Int, noOfFeatures: Int, iterations: Int, windowLength: Int, slidingLength: Int,
                   isTumbling: Boolean) {
  def execute(): Unit = {
    print("\n------------------------------\n")
    print("Config:\n")
    print("Parallelism : " + paralelizm + "\n")
    print("noOfFeatures : " + noOfFeatures + "\n")
    print("iterations : " + iterations + "\n")
    print("windowLength : " + windowLength + "\n")
    print("slidingLength : " + slidingLength + "\n")
    print("isTumbling : " + isTumbling + "\n")
    print("------------------------------\n")

    val conf = new SparkConf().setAppName("spark_performance_reduce")
    val ssc = new StreamingContext(conf, Milliseconds(1))
    val gen = new Generator()
    val data = gen.getSVMData(paralelizm, size, noOfFeatures)
    val parallelRDD = ssc.sparkContext.parallelize(data, paralelizm)
    val results = parallelRDD.map(s =>  s.split(","))
    results.foreach(s => println(s))
  }

}
