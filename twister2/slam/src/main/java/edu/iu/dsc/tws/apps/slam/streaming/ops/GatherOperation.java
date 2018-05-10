package edu.iu.dsc.tws.apps.slam.streaming.ops;

import edu.iu.dsc.tws.apps.slam.streaming.Serializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import org.bouncycastle.cert.ocsp.Req;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatherOperation {
  private static final Logger LOG = Logger.getLogger(GatherOperation.class.getName());

  private Intracomm comm;

  private Serializer serializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  private int thisTask;

  private BlockingQueue<List<Object>> result = new ArrayBlockingQueue<>(4);

  private BlockingQueue<AllGather> allGathers = new ArrayBlockingQueue<>(4);
  private BlockingQueue<Gather> gathers = new ArrayBlockingQueue<>(4);

  public GatherOperation(Intracomm comm, Serializer serializer) throws MPIException {
    this.comm = comm;
    this.serializer = serializer;
    this.worldSize = comm.getSize();
    this.thisTask = comm.getRank();
  }

//  public void iGather(Object data, int receiveTask, MessageType type) {
//    AllGather allGather = iALlGather(data, receiveTask, type);
//    allGathers.offer(allGather);
//  }

  public List<Object> gather(Object data, int receiveTask, MessageType type) {
    try {
//      LOG.log(Level.INFO, "GATHER ------------------------");
      byte[] bytes = serializer.serialize(data);

      IntBuffer countSend = MPI.newIntBuffer(worldSize);
      IntBuffer countReceive = MPI.newIntBuffer(worldSize);

      int size = bytes.length;
      ByteBuffer sendBuffer = MPI.newByteBuffer(size * 2);
      ByteBuffer receiveBuffer = MPI.newByteBuffer(size * 2 * worldSize);

      // now calculate the total number of characters
      long start = System.nanoTime();
      countSend.put(bytes.length);
      comm.allGather(countSend, 1, MPI.INT, countReceive, 1, MPI.INT);
      allGatherTime += (System.nanoTime() - start);
//      LOG.log(Level.INFO, String.format("%d ALL Gather done", thisTask));

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
      comm.gatherv(sendBuffer, bytes.length, MPI.BYTE, receiveBuffer,
          receiveSizes, displacements, MPI.BYTE, 0);
//      LOG.log(Level.INFO, String.format("%d GatherV done", thisTask));
      gatherTIme += (System.nanoTime() - start);
      List<Object> gather = new ArrayList<>();
      if (thisTask == receiveTask) {
        for (int i = 0; i < receiveSizes.length; i++) {
          byte[] c = new byte[receiveSizes[i]];
          receiveBuffer.get(c);
          Object desObj = (Object) serializer.deserialize(c);
          gather.add(desObj);
        }
      }
      return gather;
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }

  private class AllGather {
    Request request;
    IntBuffer countReceive;
    byte[] bytes;
    int receiveTask;

    public AllGather(Request request, IntBuffer countReceive, byte[] bytes, int receiveTask) {
      this.request = request;
      this.countReceive = countReceive;
      this.bytes = bytes;
      this.receiveTask = receiveTask;
    }
  }

  public void iGather(Object data, int receiveTask, MessageType type) {
    try {
//      LOG.log(Level.INFO, "GATHER ------------------------");
      byte[] bytes = serializer.serialize(data);

      IntBuffer countSend = MPI.newIntBuffer(worldSize);
      IntBuffer countReceive = MPI.newIntBuffer(worldSize);

      // now calculate the total number of characters
      long start = System.nanoTime();
      countSend.put(bytes.length);
      Request request = comm.iAllGather(countSend, 1, MPI.INT, countReceive, 1, MPI.INT);
      allGatherTime += (System.nanoTime() - start);
      LOG.log(Level.INFO, String.format("%d ALL Gather done", thisTask));
      AllGather allGather = new AllGather(request, countReceive, bytes, receiveTask);
      allGathers.offer(allGather);
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }

  private class Gather {
    Request request;

    int receiveTask;

    int[] receiveSizes;

    ByteBuffer receiveBuffer;

    public Gather(Request request, int receiveTask, int[] receiveSizes, ByteBuffer receiveBuffer) {
      this.request = request;
      this.receiveTask = receiveTask;
      this.receiveSizes = receiveSizes;
      this.receiveBuffer = receiveBuffer;
    }
  }

  private void iGather(AllGather allGather) {
    try {
      int size = allGather.bytes.length;
      ByteBuffer sendBuffer = MPI.newByteBuffer(size * 2);
      ByteBuffer receiveBuffer = MPI.newByteBuffer(size * 2 * worldSize);
      int[] receiveSizes = new int[worldSize];
      int[] displacements = new int[worldSize];
      int sum = 0;
      for (int i = 0; i < worldSize; i++) {
        receiveSizes[i] = allGather.countReceive.get(i);
        displacements[i] = sum;
        sum += receiveSizes[i];
      }
      sendBuffer.put(allGather.bytes);

      // now lets receive the process names of each rank
      Request r = comm.iGatherv(sendBuffer, allGather.bytes.length, MPI.BYTE, receiveBuffer,
          receiveSizes, displacements, MPI.BYTE, 0);
      LOG.log(Level.INFO, String.format("%d GatherV done", thisTask));

      Gather g = new Gather(r, allGather.receiveTask, receiveSizes, receiveBuffer);
      gathers.offer(g);
    } catch (MPIException e) {
      throw new RuntimeException(e);
    }
  }

  public void op() {
    try {
      AllGather r = allGathers.peek();
      if (r != null) {
        if (r.request.testStatus() != null) {
          iGather(r);
          allGathers.poll();
        }
      }

      Gather g = gathers.peek();
      if (g != null && g.request.testStatus() != null) {
        gathers.poll();
        List<Object> gather = new ArrayList<>();
        if (thisTask == g.receiveTask) {
          for (int i = 0; i < g.receiveSizes.length; i++) {
            byte[] c = new byte[g.receiveSizes[i]];
            g.receiveBuffer.get(c);
            Object desObj = (Object) serializer.deserialize(c);
            gather.add(desObj);
          }
        }
        result.offer(gather);
      }
    } catch (MPIException e) {
      e.printStackTrace();
    }
  }

  public List<Object> getResult() {
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

    GatherOperation scatterOperation = new GatherOperation(MPI.COMM_WORLD, new Serializer());
    scatterOperation.iGather(list.get(rank), 0, MessageType.OBJECT);

    final boolean[] run = {true};
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        List<Object> l = scatterOperation.getResult();
        System.out.println(String.format("%d Received list %d", rank, l.size()));
        for (int i = 0; i < l.size(); i++) {
          Simple value = (Simple) l.get(i);
          System.out.println(String.format("%d value: %s", rank, value.getVal()));
        }
        run[0] = false;
      }
    });
    t.start();
    while (run[0]) {
      scatterOperation.op();
    }

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
