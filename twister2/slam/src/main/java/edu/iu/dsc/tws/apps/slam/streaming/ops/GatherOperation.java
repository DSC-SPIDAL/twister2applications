package edu.iu.dsc.tws.apps.slam.streaming.ops;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class GatherOperation {
  private Intracomm comm;

  private KryoMemorySerializer kryoMemorySerializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  private int thisTask;

  public GatherOperation(Intracomm comm, KryoMemorySerializer kryoMemorySerializer, int worldSize, int thisTask) {
    this.comm = comm;
    this.kryoMemorySerializer = kryoMemorySerializer;
    this.worldSize = worldSize;
    this.thisTask = thisTask;
  }

  public List<Object> gather(Object data, int receiveTask, int noOfTasks, MessageType type) {
    try {
      byte[] bytes = kryoMemorySerializer.serialize(data);

      IntBuffer countSend = MPI.newIntBuffer(noOfTasks);
      IntBuffer countReceive = MPI.newIntBuffer(noOfTasks);

      int size = bytes.length;
      ByteBuffer sendBuffer = MPI.newByteBuffer(size * 2);
      ByteBuffer receiveBuffer = MPI.newByteBuffer(size * 2 * worldSize);

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
      sendBuffer.put(bytes);

      start = System.nanoTime();
      // now lets receive the process names of each rank
      MPI.COMM_WORLD.gatherv(sendBuffer, bytes.length, MPI.BYTE, receiveBuffer,
          receiveSizes, displacements, MPI.BYTE, 0);
      gatherTIme += (System.nanoTime() - start);
      List<Object> gather = new ArrayList<>();
      if (thisTask == receiveTask) {
        for (int i = 0; i < receiveSizes.length; i++) {
          byte[] c = new byte[receiveSizes[i]];
          receiveBuffer.get(c);
          Object desObj = (Object) kryoMemorySerializer.deserialize(c);
          gather.add(desObj);
        }
      }
      return gather;
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }
}
