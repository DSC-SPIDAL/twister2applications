package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class Reduce extends Collective {

  private int rank;

  private byte[] values;

  public Reduce(int size, int iterations) {
    super(size, iterations);
    values = new byte[size];
    try {
      rank = MPI.COMM_WORLD.getRank();
    } catch (MPIException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void execute() throws MPIException {
    ByteBuffer sendBuffer = MPI.newByteBuffer(size);
    ByteBuffer receiveBuffer = MPI.newByteBuffer(size);
    for (int i = 0; i < iterations; i++) {
      sendBuffer.clear();
      sendBuffer.put(values);
      MPI.COMM_WORLD.reduce(sendBuffer, receiveBuffer, size, MPI.BYTE, MPI.SUM, 0);

      if (rank == 0) {
        byte[] receiveBytes = new byte[size];
        receiveBuffer.position(size);
        receiveBuffer.flip();
        receiveBuffer.get(receiveBytes);
      }
      receiveBuffer.clear();
      sendBuffer.clear();
    }
  }
}
