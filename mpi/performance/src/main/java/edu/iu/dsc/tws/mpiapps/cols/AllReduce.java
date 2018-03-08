package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class AllReduce extends Collective {

  private long reduceTime = 0;

  private byte[] values;

  public AllReduce(int size, int iterations) {
    super(size, iterations);
    values = new byte[size];
  }

  @Override
  public void execute() throws MPIException {
    int rank = MPI.COMM_WORLD.getRank();
    ByteBuffer sendBuffer = MPI.newByteBuffer(size);
    ByteBuffer receiveBuffer = MPI.newByteBuffer(size);
    for (int i = 0; i < iterations; i++) {
      sendBuffer.clear();
      sendBuffer.put(values);
      MPI.COMM_WORLD.allReduce(sendBuffer, receiveBuffer, size, MPI.BYTE, MPI.SUM);

      if (rank == 0) {
        byte[] receiveBytes = new byte[size];
        receiveBuffer.position(size);
        receiveBuffer.flip();
        receiveBuffer.get();
        receiveBuffer.get(receiveBytes);
      }
      receiveBuffer.clear();
      sendBuffer.clear();
    }
  }
}
