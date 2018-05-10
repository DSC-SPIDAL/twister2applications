package edu.iu.dsc.tws.apps.slam.streaming.ops;

import edu.iu.dsc.tws.apps.slam.streaming.Serializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BCastOperation {
  private static final Logger LOG = Logger.getLogger(BCastOperation.class.getName());
  private Intracomm comm;

  private Serializer serializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  private int thisTask;

  private BlockingQueue<Object> result = new ArrayBlockingQueue<>(4);

  private BlockingQueue<OpRequest> requests = new ArrayBlockingQueue<>(4);

  public BCastOperation(Intracomm comm, Serializer serializer) throws MPIException {
    this.comm = comm;
    this.serializer = serializer;
    this.worldSize = comm.getSize();
    this.thisTask = comm.getRank();
  }

  public void iBcast(Object data, int bcastTask, MessageType type) {
    requests.offer(new OpRequest(data, bcastTask, type));
  }

  public Object bcast(Object data, int bcastTask, MessageType type) {
    try {
//      LOG.log(Level.INFO, "BCAST ------------------------" + thisTask + " " + bcastTask);
      IntBuffer countSend = MPI.newIntBuffer(1);
      byte[] bytes = null;
      if (thisTask == bcastTask) {
        bytes = serializer.serialize(data);
        countSend.put(bytes.length);
      }

      // now calculate the total number of characters
      long start = System.nanoTime();
      comm.bcast(countSend, 1, MPI.INT, bcastTask);
      allGatherTime += (System.nanoTime() - start);

      int receiveSize = countSend.get(0);
      ByteBuffer sendBuffer = MPI.newByteBuffer(receiveSize);
      if (thisTask == bcastTask && bytes != null) {
        sendBuffer.put(bytes);
      }

      start = System.nanoTime();
      // now lets receive the process names of each rank
      comm.bcast(sendBuffer, receiveSize, MPI.BYTE, bcastTask);
      gatherTIme += (System.nanoTime() - start);
      sendBuffer.position(0);

      byte[] c = new byte[receiveSize];
      sendBuffer.get(c);
      Object desObj = (Object) serializer.deserialize(c);
      return desObj;
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }

  public Object getResult() {
    try {
      return result.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void op() {
    OpRequest r = requests.poll();
    if (r != null) {
      Object o = bcast(r.getData(), r.getTask(), r.getType());
      result.offer(o);
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

    BCastOperation scatterOperation = new BCastOperation(MPI.COMM_WORLD, new Serializer());
    scatterOperation.bcast(list.get(rank), 0, MessageType.OBJECT);
    scatterOperation.op();
    Object l = scatterOperation.getResult();
    System.out.println(String.format("%d Received list %s", rank, l));
    MPI.Finalize();
  }
}
