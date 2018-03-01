package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

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
    IntBuffer maxSend = MPI.newIntBuffer(1);
    IntBuffer maxRecv = MPI.newIntBuffer(1);
    int rank = MPI.COMM_WORLD.getRank();

    for (int i = 0; i < iterations; i++) {
      IntBuffer sendBuffer = MPI.newIntBuffer(size);
      IntBuffer receiveBuffer = MPI.newIntBuffer(size);
      sendBuffer.clear();

      sendBuffer.put(values);

      MPI.COMM_WORLD.reduce(sendBuffer, receiveBuffer, size, MPI.INT, MPI.SUM, 0);

      if (rank == 0) {
        int receiveLength = receiveBuffer.get(0);
        int[] receiveBytes = new int[receiveLength];
        receiveBuffer.position(receiveLength + 4);
        receiveBuffer.flip();
        receiveBuffer.get();
        receiveBuffer.get(receiveBytes);
      }
      receiveBuffer.clear();
      sendBuffer.clear();
      maxRecv.clear();
      maxSend.clear();
    }
  }
}
