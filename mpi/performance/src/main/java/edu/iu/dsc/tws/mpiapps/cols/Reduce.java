package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class Reduce extends Collective {

  private int rank;

  private int[] values;

  public Reduce(int size, int iterations) {
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
    IntBuffer sendBuffer = MPI.newIntBuffer(size);
    IntBuffer receiveBuffer = MPI.newIntBuffer(size);
    sendBuffer.put(values);
    for (int i = 0; i < iterations; i++) {
      MPI.COMM_WORLD.reduce(sendBuffer, receiveBuffer, size, MPI.BYTE, MPI.SUM, 0);
      receiveBuffer.clear();
      sendBuffer.clear();
    }
  }
}
