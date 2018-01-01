#!/usr/bin/env bash

#debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
debug=
mpirun -np 1 --hostfile nodes java $debug -cp ../target/mpi-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.mpiapps.Program --collective $1 --size $2 --itr $3