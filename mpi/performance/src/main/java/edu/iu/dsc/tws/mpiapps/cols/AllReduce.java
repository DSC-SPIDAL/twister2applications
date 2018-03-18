package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class AllReduce extends Collective {

  private long reduceTime = 0;

  private int[] values;

  public AllReduce(int size, int iterations) {
    super(size, iterations);
    values = new int[size];
  }

  @Override
  public void execute() throws MPIException {
    int rank = MPI.COMM_WORLD.getRank();
    IntBuffer sendBuffer = MPI.newIntBuffer(size);
    IntBuffer receiveBuffer = MPI.newIntBuffer(size);
    for (int i = 0; i < iterations; i++) {
      sendBuffer.clear();
      sendBuffer.put(values);
      MPI.COMM_WORLD.allReduce(sendBuffer, receiveBuffer, size, MPI.BYTE, MPI.SUM);
      receiveBuffer.clear();
      sendBuffer.clear();
    }
  }
}
