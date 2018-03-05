#!/usr/bin/env bash
export SPARK_HOME=/N/u/skamburu/deploy/spark2/spark-2.1.0-bin-hadoop2.7
pathToJar=/N/u/skamburu/pulasthi_terasort/pulasthi/damds.spark/target/scala-2.11/sparkDAMDS-assembly-1.0.jar
masterURL=spark://j-112.juliet.futuresystems.org:7077
para=16*20
ec=1
em=$(( 5 * $ec ))G
sh $sparkdir/spark-submit --executor-cores $ec --executor-memory $em --class "edu.iu.dsc.tws.sparkapps.Driver" --master $masterURL $pathToJar -size $1 -iter $2 -col $3 -para $para
