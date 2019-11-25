#!/bin/bash

size=$1
itr=$2
witr=$3
agg=$4
winLen=$5
sldLen=$6
time=$7
para=$8

flink run -p ${para} -c edu.iu.dsc.tws.flinkapps.stream.windowing.WindowAggregate /home/vibhatha/github/twister2applications/flink/performance/target/flink-performance-1.0-SNAPSHOT-jar-with-dependencies.jar --size ${size} --itr ${itr} --witr ${witr} --agg ${agg} --windowLength ${winLen} --slidingLength ${sldLen} --time ${time} 
