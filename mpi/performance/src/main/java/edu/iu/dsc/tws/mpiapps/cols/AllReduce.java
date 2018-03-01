package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

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
    IntBuffer maxSend = MPI.newIntBuffer(1);
    IntBuffer maxRecv = MPI.newIntBuffer(1);
    int rank = MPI.COMM_WORLD.getRank();

    for (int i = 0; i < iterations; i++) {
      IntBuffer sendBuffer = MPI.newIntBuffer(size);
      IntBuffer receiveBuffer = MPI.newIntBuffer(size);

      sendBuffer.clear();
      sendBuffer.put(values);
      MPI.COMM_WORLD.allReduce(sendBuffer, receiveBuffer, size, MPI.INT, MPI.SUM);

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
