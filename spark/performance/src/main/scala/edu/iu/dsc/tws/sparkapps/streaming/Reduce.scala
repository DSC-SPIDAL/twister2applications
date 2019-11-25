package edu.iu.dsc.tws.sparkapps.streaming

import java.io.{FileWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Reduce(paralelizm: Int, size: Int, iterations: Int, batchSize: Int, queueDelay: Int, logPath: String) extends Serializable {

  def execute(): Unit = {
    val conf = new SparkConf().setAppName("sparkperformance_reduce")
    conf.set("spark.dynamicAllocation.enabled", "false");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.executor.extraJavaOptions", "-XX:+UseParallelGC")
    //conf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")

    val ssc = new StreamingContext(conf, Milliseconds(batchSize))
    ssc.sparkContext.setLogLevel("ERROR")
    val startTime = System.currentTimeMillis()

    // do work start
    //var status = streamSeq(ssc, paralelizm, size, iterations, queueDelay)
    streamDataObject(ssc, paralelizm, size, iterations, queueDelay)
    //streamQ(ssc, iterations, paralelizm)
    // do work stop
    val endTime = System.currentTimeMillis()
    print("\n---------------------------------------------------")
    println("\nThe number of int[] in results is : ")
    println("\nStart Time : " + System.currentTimeMillis())
    print("\nTotal time for Reduce"
      + "\n size : " + size
      + "\n iterations : " + iterations
      + "\n para : " + paralelizm
      + "\n Time : " + (endTime - startTime) / 1000
      + "\n"
    )

    val fw = new FileWriter(logPath, true)
    try {
      fw.write("reduce," + queueDelay + "," + batchSize + "," + size + "," + iterations + "," + paralelizm + ","
        + (endTime - startTime) / 1000.0 + "\n")
    } finally {
      fw.close()
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(11)
  }

  def streamSeq(ssc: StreamingContext, parallelizm: Int, size: Int, iterations: Int, queueDelay: Int): Boolean = {
    var status = false
    val rddQueu = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueu)
    val result = inputStream.reduce((x, y) => x + y)
    result.print()
    for (i <- 1 to iterations) {
      rddQueu += ssc.sparkContext.makeRDD(1 to size * parallelizm, parallelizm)
      if (i == iterations) {
        status = true
      }
      Thread.sleep(queueDelay)
    }
    status
  }

  def streamDataObject(ssc: StreamingContext, parallelizm: Int, size: Int, iterations: Int, queueDelay: Int): Unit = {

    val rddQueue = new mutable.Queue[RDD[DataObject]]
    val inputStream = ssc.queueStream(rddQueue)
    val reduceResult  = inputStream.reduce((p1,p2) => update(p1, p2))
    reduceResult.foreachRDD((rdd) => rdd.foreach((p) => println(p.x + ":" + p.y)))
    //inputStream.count().print()

    var count = 0
    for (i <- 1 to iterations) {
      val dataObject = new DataObject(System.currentTimeMillis(), 1 to size * parallelizm)
      val seq1: Seq[DataObject] = Seq[DataObject](dataObject)
      rddQueue += ssc.sparkContext.makeRDD(seq1, paralelizm)
      Thread.sleep(queueDelay)
    }

  }

  def update(a: DataObject, b: DataObject): DataObject = {
    a.y = Math.min(a.y, b.y)
    a.x = a.x.zip(b.x).map { case (x, y) => x + y }
    printDataObject(a)
    a
  }

  def printDataObject(dataObject: DataObject): Unit = {
    println(dataObject.y + ":" + dataObject.x)
  }





  def streamQ(ssc: StreamingContext, iterations: Int, paralelizm: Int): Unit = {
    val rddQueue = new mutable.Queue[RDD[Array[Int]]]
    val inputStream = ssc.queueStream(rddQueue)

    val resultReduce = inputStream.reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
    resultReduce.print()
    //val resultReduce1 = inputStream.reduce((ar1,ar2) => ar1.zip(ar2).map((p)=>p._1 + p._2))
    //val resultReduce = inputStream.reduce((p1,p2) => )
    //val result = inputStream.reduce((p1, p2) => p1 + p2)
    //val resultCollect = inputStream.map(p=>p).count()

    //resultCollect.foreachRDD((rdd) => rdd.foreach((p) => println("count=" + p + "**")))
    //resultReduce.foreachRDD((rdd) => rdd.foreach((p) => println("reduce=" + p + "**")))
    //resultReduce.foreachRDD((rdd) => rdd.foreach(p => p.foreach(q=> print(q + ","))))

    //resultReduce.foreachRDD((rdd) => rdd.collect().foreach(p1 => p1.foreach(p2 => println("reduce=" + p2))))

    var count = 0;

    for (i <- 1 to iterations) {
      val ar1: ArrayBuffer[Int] = ArrayBuffer[Int](size * paralelizm)
      for (j <- 1 to size * paralelizm) {
        ar1 += j
      }
      //      for (k <- 1 to size * paralelizm) {
      //        print(ar1(k) + ",")
      //      }
      //      println("--------------------------")
      val ar2 = ar1.toArray
      val seq1: Seq[Array[Int]] = Seq[Array[Int]](ar2)
      rddQueue += ssc.sparkContext.makeRDD(seq1, paralelizm)
      //val a = rddQueue.dequeue()
      //println("\n *****************" + a +"******************")
      //a.collect().foreach((p) => p.foreach((p1) => println("\n#####"+p1+"####")))
      //rddQueue.enqueue(a)
      Thread.sleep(queueDelay)
    }

    //    for (i <- 1 to iterations) {
    ////      val seq1: Seq[Int] = Seq[Int]()
    ////      for (j <- 1 to size * paralelizm) {
    ////        seq1:+count
    ////        count = count + 1
    ////      }
    //      rddQueue += ssc.sparkContext.makeRDD(1 to size * paralelizm, paralelizm)
    //      Thread.sleep(queueDelay)
    //    }
    //
    //        for (i <- 1 to iterations) {
    //          val seq1: Seq[DataObject] = Seq[DataObject]()
    //          for (j <- 1 to size * paralelizm) {
    //            val dataObject = new DataObject(count)
    //            seq1:+dataObject
    //            count = count + 1
    //          }
    //          rddQueue += ssc.sparkContext.makeRDD(seq1, paralelizm)
    //          Thread.sleep(queueDelay)
    //        }

    //    for ( i <- 1 to iterations) {
    //      val seq1:Seq[ArrayBuffer[Int]] = Seq[ArrayBuffer[Int]]()
    //      val ar1:ArrayBuffer[Int] = ArrayBuffer[Int]()
    //      for (j <- 1 to size * paralelizm) {
    //        ar1 += j
    //      }
    //      seq1:+ar1
    //      rddQueue += ssc.sparkContext.makeRDD(seq1, paralelizm)
    //      Thread.sleep(queueDelay)
    //    }
  }


  class DataObject(yc: Long, xc: Seq[Int]) extends Serializable {
    var y: Long = yc
    var x: Seq[Int] = xc

    def update(dataObject: DataObject): DataObject = {
      this.y = Math.min(y, dataObject.y)
      this.x = 100 to 116
      this
    }
  }

}