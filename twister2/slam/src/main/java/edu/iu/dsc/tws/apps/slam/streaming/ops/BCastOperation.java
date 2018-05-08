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

public class BCastOperation {
  private Intracomm comm;

  private KryoMemorySerializer kryoMemorySerializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  private int thisTask;

  public BCastOperation(Intracomm comm, KryoMemorySerializer kryoMemorySerializer) throws MPIException {
    this.comm = comm;
    this.kryoMemorySerializer = kryoMemorySerializer;
    this.worldSize = comm.getSize();
    this.thisTask = comm.getRank();
  }

  public Object bcast(Object data, int bcastTask, MessageType type) {
    try {
      IntBuffer countSend = MPI.newIntBuffer(1);
      byte[] bytes = null;
      if (thisTask == bcastTask) {
        bytes = kryoMemorySerializer.serialize(data);
        countSend.put(bytes.length);
      }

      // now calculate the total number of characters
      long start = System.nanoTime();
      MPI.COMM_WORLD.bcast(countSend, worldSize, MPI.INT, bcastTask);
      allGatherTime += (System.nanoTime() - start);

      int receiveSize = countSend.get(0);
      ByteBuffer sendBuffer = MPI.newByteBuffer(receiveSize);
      if (thisTask == bcastTask && bytes != null) {
        sendBuffer.put(bytes);
      }

      start = System.nanoTime();
      // now lets receive the process names of each rank
      MPI.COMM_WORLD.bcast(sendBuffer, receiveSize, MPI.BYTE, bcastTask);
      gatherTIme += (System.nanoTime() - start);
      sendBuffer.position(0);

      byte[] c = new byte[receiveSize];
      sendBuffer.get(c);
      Object desObj = (Object) kryoMemorySerializer.deserialize(c);
      return desObj;
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws MPIException {
    MPI.Init(args);
    int size = MPI.COMM_WORLD.getSize();
    int rank = MPI.COMM_WORLD.getRank();
    List<String> list = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      list.add("Hello: " + i);
    }

    BCastOperation scatterOperation = new BCastOperation(MPI.COMM_WORLD, new KryoMemorySerializer());
    Object l = scatterOperation.bcast(list.get(rank), 0, MessageType.OBJECT);
    System.out.println(String.format("%d Received list %s", rank, l));
    MPI.Finalize();
  }
}
