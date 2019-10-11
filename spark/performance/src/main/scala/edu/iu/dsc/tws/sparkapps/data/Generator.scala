package edu.iu.dsc.tws.sparkapps.data

import java.util.Random

import edu.iu.dsc.tws.apps.common.RandomString

/**
  * Created by pulasthi on 2/20/18.
  */
class Generator {
  private var randomString: RandomString = null

  def getStringData(paralelizm: Int, size: Int,iterations: Int): List[String] = {
    var data: List[String] = List[String]()
    val totalStrings = paralelizm * iterations;
    randomString = new RandomString(size, new Random(System.nanoTime), RandomString.alphanum)
    for( i <- 1 to totalStrings){
      data :+ randomString.nextString()
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
