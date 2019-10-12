package edu.iu.dsc.tws.sparkapps.svm

class StreamingSVM(paralelizm: Int, noOfFeatures: Int, iterations: Int, windowLength: Int, slidingLength: Int,
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
  }

}
