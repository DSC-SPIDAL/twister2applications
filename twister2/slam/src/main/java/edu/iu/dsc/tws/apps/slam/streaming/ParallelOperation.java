package edu.iu.dsc.tws.apps.slam.streaming;

import mpi.MPI;

import java.util.List;

public class ParallelOperation {

  public static List<Object> gather() {
//    byte[] bytes = kryoSerializer.serialize(next);
////      LOG.log(Level.INFO, String.format("%d Byte size: %d", rank, bytes.length));
//    // now calculate the total number of characters
//    start = System.nanoTime();
//    countSend.put(bytes.length);
//    MPI.COMM_WORLD.allGather(countSend, 1, MPI.INT, countReceive, 1, MPI.INT);
//    allGatherTime += (System.nanoTime() - start);
//
//    int[] receiveSizes = new int[worldSize];
//    int[] displacements = new int[worldSize];
//    int sum = 0;
//    for (int i = 0; i < worldSize; i++) {
//      receiveSizes[i] = countReceive.get(i);
//      displacements[i] = sum;
//      sum += receiveSizes[i];
//    }
//
//    sendBuffer.clear();
//    sendBuffer.put(bytes);
//
//    start = System.nanoTime();
//    // now lets receive the process names of each rank
//    MPI.COMM_WORLD.gatherv(sendBuffer, bytes.length, MPI.BYTE, receiveBuffer,
//        receiveSizes, displacements, MPI.BYTE, 0);
//    gatherTIme += (System.nanoTime() - start);
//    if (rank == 0) {
//      for (int i = 0; i < receiveSizes.length; i++) {
//        byte[] c = new byte[receiveSizes[i]];
//        receiveBuffer.get(c);
//        String deserialize = (String) kryoSerializer.deserialize(c);
////          System.out.println(deserialize);
//      }
//    }
    return null;
  }
}
