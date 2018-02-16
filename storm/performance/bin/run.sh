#!/usr/bin/env bash

flink run -m  j-067:6123 -p $1 -c edu.iu.dsc.tws.flinkapps.Program  /N/u/skamburu/twister2/twister2applications/flink/performance/target/flink-performance-1.0-SNAPSHOT.jar -size $2 -itr $3 -col $4 -stream $5