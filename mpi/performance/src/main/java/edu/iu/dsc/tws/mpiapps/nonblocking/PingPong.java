package edu.iu.dsc.tws.mpiapps.nonblocking;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.MPI;
import mpi.MPIException;

import java.nio.ByteBuffer;

public class PingPong extends Collective {
  public PingPong(int size, int iterations) {
    super(size, iterations);
  }

  @Override
  public void execute() throws MPIException {
    int rank = MPI.COMM_WORLD.getRank();

    ByteBuffer sendBuffer = MPI.newByteBuffer(size);
    ByteBuffer recvBuffer = MPI.newByteBuffer(size);
    long sum = 0;
    int currentItr = 0;
    while (currentItr < iterations) {
      sendBuffer.clear();
      recvBuffer.clear();
      if (rank == 0) {
        sendBuffer.putLong(System.nanoTime());
        MPI.COMM_WORLD.send(sendBuffer, size, MPI.BYTE, 1, 0);
      }
      if (rank == 1) {
        MPI.COMM_WORLD.recv(recvBuffer, size, MPI.BYTE, 0, 0);
        recvBuffer.position(size);
        recvBuffer.flip();
        long recv = recvBuffer.getLong();

        sendBuffer.putLong(recv);
        MPI.COMM_WORLD.send(sendBuffer, 8, MPI.BYTE, 0, 0);
      }
      if (rank == 0) {
        MPI.COMM_WORLD.recv(recvBuffer, 8, MPI.BYTE, 1, 0);
        recvBuffer.position(size);
        recvBuffer.flip();
        long recv = recvBuffer.getLong();
        sum += (System.nanoTime() - recv);
      }
      currentItr++;
    }

    if (rank == 0) {
      System.out.println("Average latency in nano secs: " + (sum / iterations));
    }
  }
}
