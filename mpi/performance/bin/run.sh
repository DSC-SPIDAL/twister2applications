#!/usr/bin/env bash

#debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
debug=
op="--mca btl ^openib --mca btl_tcp_if_include eth0 --mca btl_base_verbose 30"
op="--mca btl ^openib --mca btl_tcp_if_include eth0"
#op="--mca btl ^openib"
#op="--mca btl openib,self,vader --mca btl_base_verbose 30"
op="--mca btl openib,self,vader"
mpirun --bind-to core --report-bindings  $op -np $1 --hostfile nodes java -Xmx6G -Xms6G -cp ../target/mpi-performance-1.0-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.mpiapps.Program --collective $2 --size $3 --itr $4
