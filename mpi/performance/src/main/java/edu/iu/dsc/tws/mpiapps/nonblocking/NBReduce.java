package edu.iu.dsc.tws.mpiapps.nonblocking;

import edu.iu.dsc.tws.mpiapps.Collective;
import edu.iu.dsc.tws.mpiapps.KryoSerializer;
import edu.iu.dsc.tws.mpiapps.data.IntData;
import mpi.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class NBReduce extends Collective {
  private KryoSerializer kryoSerializer;

  private long allReduceTime = 0;

  private long reduceTime = 0;

  public NBReduce(int size, int iterations) {
    super(size, iterations);
    this.kryoSerializer = new KryoSerializer();
    this.kryoSerializer.init(new HashMap());
  }

  private class RequestInfo {
    ByteBuffer sendBuffer;
    ByteBuffer recvBuffer;
    byte[] data;
    Request request;

    public RequestInfo(ByteBuffer sendBuffer, ByteBuffer recvBuffer, byte[] data, Request request) {
      this.sendBuffer = sendBuffer;
      this.recvBuffer = recvBuffer;
      this.data = data;
      this.request = request;
    }
  }

  @Override
  public void execute() throws MPIException {
    int rank = MPI.COMM_WORLD.getRank();
    byte[] bytes = new byte[size];
    int maxPending = 1;
    Queue<ByteBuffer> sendBuffers = new ArrayBlockingQueue<>(maxPending);
    Queue<ByteBuffer> recvBuffers = new ArrayBlockingQueue<>(maxPending);

    Queue<RequestInfo> receiveRequestQueue = new ArrayBlockingQueue<>(maxPending);

    for (int i = 0; i < maxPending; i++) {
      sendBuffers.add(MPI.newByteBuffer(size));
      recvBuffers.add(MPI.newByteBuffer(size));
    }

    int completed = 0;
    while (completed < iterations) {
      if (receiveRequestQueue.size() < maxPending) {
        ByteBuffer sendBuffer = sendBuffers.poll();
        ByteBuffer receiveBuffer = recvBuffers.poll();

        if (sendBuffer == null || receiveBuffer == null) {
          throw new RuntimeException("Buffer null");
        }
        sendBuffer.clear();
        receiveBuffer.clear();

        Request dataR = MPI.COMM_WORLD.iReduce(sendBuffer, receiveBuffer, size, MPI.BYTE, MPI.SUM, 0);
        receiveRequestQueue.add(new RequestInfo(sendBuffer, receiveBuffer, bytes, dataR));
      }

      while (receiveRequestQueue.size() >= maxPending) {
        RequestInfo receiveRequest = receiveRequestQueue.peek();
        if (receiveRequest != null && receiveRequest.request != null && receiveRequest.request.testStatus() != null) {
          RequestInfo info = receiveRequestQueue.poll();

          ByteBuffer sendBuffer = info.sendBuffer;
          ByteBuffer receiveBuffer = info.recvBuffer;

          sendBuffer.clear();
          if (rank == 0) {
            int receiveLength = receiveBuffer.getInt(0);
            byte[] receiveBytes = new byte[receiveLength];
            receiveBuffer.position(receiveLength + 4);
            receiveBuffer.flip();
            receiveBuffer.get(receiveBytes);
          }
          receiveBuffer.clear();
          sendBuffer.clear();
          sendBuffers.add(sendBuffer);
          recvBuffers.add(receiveBuffer);

          completed++;
        } else{
          break;
        }
      }
    }

    if (rank == 0) {
      System.out.println("Final time: " + allReduceTime / 1000000 + " ," + reduceTime / 1000000);
    }
  }
}
