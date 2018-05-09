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

public class ScatterOperation {
  private Intracomm comm;

  private Serializer serializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  private int thisTask;

  private BlockingQueue<Object> result = new ArrayBlockingQueue<>(4);

  private BlockingQueue<Request> requests = new ArrayBlockingQueue<>(4);

  public ScatterOperation(Intracomm comm, Serializer serializer) throws MPIException {
    this.comm = comm;
    this.serializer = serializer;
    this.worldSize = comm.getSize();
    this.thisTask = comm.getRank();
  }

  public void iScatter(List data, int scatterTask, MessageType type) {
    requests.offer(new Request(data, scatterTask, type));
  }

  public Object scatter(List data, int scatterTask, MessageType type) {
    try {
      IntBuffer countSend = MPI.newIntBuffer(worldSize);
      int total = 0;
      List<byte []> dataList = new ArrayList<>();
      if (thisTask == scatterTask) {
        for (Object o : data) {
          byte[] bytes = serializer.serialize(o);
          countSend.put(bytes.length);
          total += bytes.length;
          dataList.add(bytes);
        }
      }

      long start = System.nanoTime();
      comm.bcast(countSend, worldSize, MPI.INT, scatterTask);
      total = 0;
      for (int i = 0; i < worldSize; i++) {
        int total1 = countSend.get(i);
        total += total1;
        System.out.println(String.format("%d size: %d", thisTask, total1));
      }

      ByteBuffer sendBuffer = MPI.newByteBuffer(total * 2);
      if (thisTask == scatterTask) {
        for (int i = 0; i < worldSize; i++) {
          sendBuffer.put(dataList.get(i));
        }
      }
      // now calculate the total number of characters
      allGatherTime += (System.nanoTime() - start);

      int[] receiveSizes = new int[worldSize];
      int[] displacements = new int[worldSize];
      int sum = 0;
      for (int i = 0; i < worldSize; i++) {
        receiveSizes[i] = countSend.get(i);
        displacements[i] = sum;
        sum += receiveSizes[i];
      }

      start = System.nanoTime();

      ByteBuffer receiveBuffer = MPI.newByteBuffer(receiveSizes[thisTask]);
      // now lets receive the process names of each rank
      comm.scatterv(sendBuffer, receiveSizes, displacements, MPI.BYTE, receiveBuffer, receiveSizes[thisTask], MPI.BYTE, scatterTask);
      gatherTIme += (System.nanoTime() - start);
      byte[] c = new byte[receiveSizes[thisTask]];
      receiveBuffer.get(c);
      Object desObj = (Object) serializer.deserialize(c);
      return desObj;
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }

  public void op() {
    Request r = requests.poll();
    if (r != null) {
      Object l = scatter((List) r.getData(), r.getTask(), r.getType());
      result.offer(l);
    }
  }

  public Object getResult() {
    try {
      return result.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws MPIException {
    MPI.Init(args);
    int size = MPI.COMM_WORLD.getSize();
    int rank = MPI.COMM_WORLD.getRank();
    List<Simple> list = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      list.add(new Simple("Hello: " + i));
    }

    ScatterOperation scatterOperation = new ScatterOperation(MPI.COMM_WORLD, new Serializer());
    Object value = scatterOperation.scatter(list, 0, MessageType.OBJECT);
    System.out.println(String.format("%d value: %s", rank, ((Simple)value).getVal()));

    MPI.Finalize();
  }

  private static class Simple {
    private String val;

    public Simple(String val) {
      this.val = val;
    }

    public Simple() {
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }
}
