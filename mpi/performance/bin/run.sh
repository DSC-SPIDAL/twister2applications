#!/usr/bin/env bash

mpirun -np 4 --hostfile nodes java -cp ../target/mpi-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.mpiapps.Program --collective $1 --size $2 --itr $3