package edu.iu.dsc.tws.sparkapps.data

import java.util.Random

import edu.iu.dsc.tws.apps.common.RandomString

import scala.collection.mutable

/**
  * Created by pulasthi on 2/20/18.
  */
class Generator {
  private var randomString: RandomString = null
  private var randomDouble: Random = null

  def getStringData(paralelizm: Int, size: Int,iterations: Int): List[String] = {
    var data: List[String] = List[String]()
    val totalStrings = paralelizm * iterations;
    randomString = new RandomString(size, new Random(System.nanoTime), RandomString.alphanum)
    for( i <- 1 to totalStrings){
      data :+ randomString.nextString()
    }
    data
  }

  def getStringSequence(paralelizm: Int, size: Int,iterations: Int): Seq[String] = {
    var data: Seq[String] = Seq[String]()
    val totalStrings = paralelizm * iterations;
    randomString = new RandomString(size, new Random(System.nanoTime), RandomString.alphanum)
    for( i <- 1 to totalStrings){
      data :+ randomString.nextString()
    }
    data
  }

  def getStringDataObjects(size: Int): Seq[DataObject] = {
    var data: Seq[DataObject] = Seq[DataObject]()
    val totalStrings = size
    randomString = new RandomString(size, new Random(System.nanoTime), RandomString.alphanum)
    for( i <- 1 to totalStrings){
      data :+ randomString.nextString()
    }
    data
  }

  def getStringQueue(paralelizm: Int, size: Int,iterations: Int): mutable.Queue[String] = {
    var data: mutable.Queue[String] = mutable.Queue[String]()
    val totalStrings = paralelizm * iterations;
    randomString = new RandomString(size, new Random(System.nanoTime), RandomString.alphanum)
    for( i <- 1 to totalStrings){
      data :+ randomString.nextString()
    }
    data
  }



  def getSVMData(paralelizm: Int, size: Int,noOfFeatures: Int): List[String] = {
    var data: List[String] = List[String]()
    randomDouble = new Random()
    for (j <- 1 to size) {
      var str = ""
      for (i <- 1 to noOfFeatures) {
        str += randomDouble.nextGaussian() + ","
      }
      data:+ str
    }
    data
  }

  def getIntData(paralelizm: Int, size: Int,iterations: Int): List[Int] = {
    var data2: List[Int] = List[Int]()
    val totalInts = paralelizm * iterations * size;
    for( i <- 1 to totalInts){
      data2 :+ i
    }
    data2
  }
}

class DataObject(yc: Int) extends Serializable {
  var y: Int = yc
}