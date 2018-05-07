package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import mpi.Intracomm;
import mpi.MPI;

import java.util.List;

public class GatherOperation {
  private Intracomm comm;

  private KryoMemorySerializer kryoMemorySerializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  public List<Object> gather(Object data, int receiveTask) {
    byte[] bytes = kryoMemorySerializer.serialize(data);
    // now calculate the total number of characters
    long start = System.nanoTime();
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
      }
    }
    return null;
  }
}
