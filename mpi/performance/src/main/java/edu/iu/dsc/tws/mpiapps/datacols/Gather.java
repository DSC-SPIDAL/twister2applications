package edu.iu.dsc.tws.mpiapps.datacols;

import edu.iu.dsc.tws.apps.common.RandomString;
import edu.iu.dsc.tws.mpiapps.Collective;
import edu.iu.dsc.tws.mpiapps.KryoSerializer;
import mpi.MPI;
import mpi.MPIException;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.logging.Logger;

public class Gather extends Collective {
  private static final Logger LOG = Logger.getLogger(Gather.class.getName());

  private RandomString randomString;

  private KryoSerializer kryoSerializer;

  private long allGatherTime = 0;

  private long gatherTIme = 0;

  public Gather(int size, int iterations) {
    super(size, iterations);
    this.randomString = new RandomString(size);
    this.kryoSerializer = new KryoSerializer();
    this.kryoSerializer.init(new HashMap());
  }

  public void execute() throws MPIException {
    IntBuffer countSend = MPI.newIntBuffer(1);
    int worldSize = MPI.COMM_WORLD.getSize();
    int rank = MPI.COMM_WORLD.getRank();
    ByteBuffer sendBuffer = MPI.newByteBuffer(size * 2);
    ByteBuffer receiveBuffer = MPI.newByteBuffer(size * 2 * worldSize);
    IntBuffer countReceive = MPI.newIntBuffer(worldSize);
    String next = randomString.nextString();
    long start = 0;
    for (int itr = 0; itr < iterations; itr++) {
      byte[] bytes = kryoSerializer.serialize(next);
//      LOG.log(Level.INFO, String.format("%d Byte size: %d", rank, bytes.length));
      // now calculate the total number of characters
      start = System.nanoTime();
      countSend.put(bytes.length);
      MPI.COMM_WORLD.allGather(countSend, 1, MPI.INT, countReceive, 1, MPI.INT);
      allGatherTime += (System.nanoTime() - start);

      int[] receiveSizes = new int[worldSize];
      int[] displacements = new int[worldSize];
      int sum = 0;
      for (int i = 0; i < worldSize; i++) {
        receiveSizes[i] = countReceive.get(i);
        displacements[i] = sum;
        sum += receiveSizes[i];
//        LOG.log(Level.INFO, String.format("Process %d: receive size %d", rank, receiveSizes[i]));
      }

      sendBuffer.clear();
      sendBuffer.put(bytes);

      start = System.nanoTime();
      // now lets receive the process names of each rank
      MPI.COMM_WORLD.gatherv(sendBuffer, bytes.length, MPI.BYTE, receiveBuffer,
          receiveSizes, displacements, MPI.BYTE, 0);
      gatherTIme += (System.nanoTime() - start);
      if (rank == 0) {
        for (int i = 0; i < receiveSizes.length; i++) {
          byte[] c = new byte[receiveSizes[i]];
          receiveBuffer.get(c);
          String deserialize = (String) kryoSerializer.deserialize(c);
//          System.out.println(deserialize);
        }
      }

      receiveBuffer.clear();
      sendBuffer.clear();
      countReceive.clear();
      countSend.clear();
    }

    if (rank == 0) {
      System.out.println("Final time: " + allGatherTime / 1000000 + " ," + gatherTIme / 1000000);
    }
  }
}
