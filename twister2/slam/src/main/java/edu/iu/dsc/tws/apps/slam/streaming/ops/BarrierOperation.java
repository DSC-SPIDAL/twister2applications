package edu.iu.dsc.tws.apps.slam.streaming.ops;

import edu.iu.dsc.tws.apps.slam.streaming.Serializer;
import mpi.Intracomm;
import mpi.MPIException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BarrierOperation {
  private Intracomm comm;

  private Serializer serializer;

  private long allGatherTime = 0;

  private int worldSize;

  private long gatherTIme;

  private long getAllGatherTime;

  private int thisTask;

  private BlockingQueue<Object> result = new ArrayBlockingQueue<>(4);

  private BlockingQueue<OpRequest> requests = new ArrayBlockingQueue<>(4);

  public BarrierOperation(Intracomm comm, Serializer serializer) throws MPIException {
    this.comm = comm;
    this.serializer = serializer;
    this.worldSize = comm.getSize();
    this.thisTask = comm.getRank();
  }

  public void iBarrier() {
    requests.offer(new OpRequest(null, 0, null));
  }

  public boolean barrier() {
    try {
      comm.barrier();
      return true;
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
      Object o = barrier();
      result.offer(o);
    }
  }
}
