package edu.iu.dsc.tws.sparkapps.test

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class GeneralTest(paralelizm: Int, size: Int, iterations: Int, batchSize: Int, queueDelay: Int, logPath: String) {
  def execute(): Unit = {

    val conf = new SparkConf().setAppName("spark_performance_reduce")
    conf.set("spark.dynamicAllocation.enabled", "false");

    val sc = new SparkContext(conf)

    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    val ssc = new StreamingContext(sc, Milliseconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    val rddQueue = new mutable.Queue[RDD[ArrayBuffer[Int]]]
//    val seqA = arrayTest(ssc, size, paralelizm)
//    val a = 1 to 10
//    println(a.take(5))

    //val list = arrayAdditionTest(ssc, size, paralelizm)
    simpleArrayReduce(ssc)
    //print(list)


    ssc.start()
    ssc.stop()

  }

  def arrayTest(ssc: StreamingContext, size: Int, parallelizm: Int): Seq[ArrayBuffer[Int]] = {

    val ar1: ArrayBuffer[Int] = ArrayBuffer[Int](size * parallelizm)
    for (j <- 1 to size * paralelizm) {
      ar1 += j
    }
    for (k <- 1 to size * paralelizm) {
      print(ar1(k) + ",")
    }
    println("\n--------------------------")
    val seq1: Seq[ArrayBuffer[Int]] = Seq[ArrayBuffer[Int]](ar1)

    seq1
  }

  def arrayAdditionTest(ssc: StreamingContext, size: Int, parallelizm: Int): ListBuffer[Seq[ArrayBuffer[Int]]] = {

    val ar1: ArrayBuffer[Int] = ArrayBuffer[Int](size * parallelizm)
    val ar2: ArrayBuffer[Int] = ArrayBuffer[Int](size * parallelizm)
    for (j <- 1 to size * paralelizm) {
      ar1 += j
      ar2 += j * 10
    }
    val ad = ar1.zip(ar2).map((p)=>p._1 + p._2).result()
    print(ad)

//    for (k <- 1 to size * paralelizm) {
//      print(ar1(k) + ",")
//    }
//    println("\n--------------------------")
    val seq1: Seq[ArrayBuffer[Int]] = Seq[ArrayBuffer[Int]](ar1)
    val seq2: Seq[ArrayBuffer[Int]] = Seq[ArrayBuffer[Int]](ar2)
    val arrs: ListBuffer[Seq[ArrayBuffer[Int]]] = ListBuffer[Seq[ArrayBuffer[Int]]]()
    arrs += seq1
    arrs += seq2

    arrs
  }

  def simpleArrayReduce(ssc: StreamingContext): Unit = {

    val rdd1 = ssc.sparkContext.parallelize(Seq(Array(1.0, 2.0), Array(3.0, 4.0)))
    val reduce = rdd1.reduce((p,q) => p.zip(q).map(k=> k._1 + k._2))
    reduce.foreach((a) => print(a + ","))

  }

  def doFileStream(ssc: StreamingContext): Unit = {
    print("---------Test File Stream----------------")
    val streamLines = ssc.textFileStream("/home/vibhatha/data/streaming/svm/ijcnn1")

    val reducedValues = streamLines
      .map(s => (s, 1))
      .reduce((s1, s2) => (s1._1 + s2._1, s1._2 + s1._2))

    //reducedValues.foreachRDD((s1, i1) => (s1.foreach((e) => (println(e._1 + "::" + e._2)))))
    //reducedValues.foreachRDD((rdd, time) => rdd.foreach((k) => print(k._1 + "::" + k._2)))
    reducedValues.count()

    reducedValues.print()

    //weatherTemps1Hour.foreachRDD((s) => println(s))

    /**
     * Batch Method
     **/
    //val lines = sc.textFile("/home/vibhatha/data/streaming/svm/ijcnn1/training.csv", 1)
    //lines.foreach(s=> print(s+"--------------------------"))


    println("\n-----------------------------------------------------------")
  }

  // declaration and definition of function
  def functionToAdd(a: Int, b: Int): Int = {

    var sum: Int = 0
    sum = a + b

    // returning the value of sum
    return sum
  }


}
