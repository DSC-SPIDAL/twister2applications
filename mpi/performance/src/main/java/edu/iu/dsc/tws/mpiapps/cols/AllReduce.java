package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

import java.nio.DoubleBuffer;

public class AllReduce extends Collective {

  private long reduceTime = 0;

  private double[] values;

  public AllReduce(int size, int iterations) {
    super(size, iterations);
    values = new double[size];
  }

  @Override
  public void execute() throws MPIException {
    int rank = MPI.COMM_WORLD.getRank();
    DoubleBuffer sendBuffer = MPI.newDoubleBuffer(size);
    DoubleBuffer receiveBuffer = MPI.newDoubleBuffer(size);
    for (int i = 0; i < iterations; i++) {
      sendBuffer.clear();
      sendBuffer.put(values);
      MPI.COMM_WORLD.allReduce(sendBuffer, receiveBuffer, size, MPI.DOUBLE, MPI.SUM);
      receiveBuffer.clear();
      sendBuffer.clear();
    }
  }
}
