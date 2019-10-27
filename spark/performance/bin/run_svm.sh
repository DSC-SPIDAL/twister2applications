#!/bin/bash
#export SPARK_HOME=/N/u/skamburu/deploy/spark2/spark-2.1.0-bin-hadoop2.7
pathToJar=/home/vibhatha/github/twister2applications/spark/performance/target/spark-performance-1.0-SNAPSHOT-jar-with-dependencies.jar
masterURL=spark://localhost:7077
para=4
ec=1
em=$(( 1 * $ec ))G
spark-submit --executor-cores $ec --executor-memory $em --class "edu.iu.dsc.tws.sparkapps.svm.SVMDriver" --master $masterURL $pathToJar -size $1 -iter $2 -features $3 -windowLength $4 -slidingLength $5 -windowType $6 -para $para
