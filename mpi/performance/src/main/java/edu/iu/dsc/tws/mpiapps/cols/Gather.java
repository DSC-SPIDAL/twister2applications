package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.MPI;
import mpi.MPIException;

import java.nio.IntBuffer;

public class Gather extends Collective {
  private int rank;

  private int[] values;

  public Gather(int size, int iterations) {
    super(size, iterations);
    values = new int[size];
    try {
      rank = MPI.COMM_WORLD.getRank();
    } catch (MPIException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void execute() throws MPIException {
    int worldSize = MPI.COMM_WORLD.getSize();
    IntBuffer sendBuffer = MPI.newIntBuffer(size);
    IntBuffer receiveBuffer = MPI.newIntBuffer(size * worldSize);
    for (int i = 0; i < iterations; i++) {
      sendBuffer.put(values);
      MPI.COMM_WORLD.gather(sendBuffer, size, MPI.INT, receiveBuffer, size, MPI.INT, 0);
      receiveBuffer.clear();
      sendBuffer.clear();
    }
  }
}
