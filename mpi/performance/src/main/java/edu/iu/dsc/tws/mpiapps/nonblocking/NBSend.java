package edu.iu.dsc.tws.mpiapps.nonblocking;

import edu.iu.dsc.tws.mpiapps.Collective;
import edu.iu.dsc.tws.mpiapps.KryoSerializer;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import mpi.Status;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

public class NBSend extends Collective {
  private static final Logger LOG = Logger.getLogger(NBSend.class.getName());

  private KryoSerializer kryoSerializer;

  public NBSend(int size, int iterations) {
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

    if (rank == 0) {
      sendProcess();
    } else {
      receiveProcess();
    }
  }

  public void sendProcess() throws MPIException {
    byte[] bytes = new byte[size];
    int maxPending = 16;
    Queue<ByteBuffer> sendBuffers = new ArrayBlockingQueue<>(maxPending);

    Queue<RequestInfo> reqestQueue = new ArrayBlockingQueue<>(maxPending);

    for (int i = 0; i < maxPending; i++) {
      sendBuffers.add(MPI.newByteBuffer(size));
    }

    int completed = 0;
    int sent = 0;
    while (completed < iterations) {
      if (sendBuffers.size() > 0) {
        ByteBuffer sendBuffer = sendBuffers.poll();
        sendBuffer.clear();
        sendBuffer.put(bytes);
        sendBuffer.putInt(0, sent);
        sent++;

        Request dataR = MPI.COMM_WORLD.iSend(sendBuffer, size, MPI.BYTE, 1, 0);
        reqestQueue.add(new RequestInfo(sendBuffer, null, bytes, dataR));
      }

      while (reqestQueue.size() > 0) {
        RequestInfo receiveRequest = reqestQueue.peek();
        if (receiveRequest != null && receiveRequest.request != null && receiveRequest.request.testStatus() != null) {
          RequestInfo info = reqestQueue.poll();
          ByteBuffer sendBuffer = info.sendBuffer;
          sendBuffer.clear();
          sendBuffers.add(sendBuffer);
          completed++;
        } else{
          break;
        }
      }
    }
  }

  public void receiveProcess() throws MPIException {
    int maxPending = 16;
    Queue<ByteBuffer> recvBuffers = new ArrayBlockingQueue<>(maxPending);
    byte[] receiveBytes = new byte[size];

    Queue<RequestInfo> requestQueue = new ArrayBlockingQueue<>(maxPending);

    for (int i = 0; i < maxPending; i++) {
      recvBuffers.add(MPI.newByteBuffer(size));
    }

    int completed = 0;
    while (completed < iterations) {
      if (recvBuffers.size() > 0) {
        ByteBuffer receiveBuffer = recvBuffers.poll();

        Request dataR = MPI.COMM_WORLD.iRecv(receiveBuffer, size, MPI.BYTE, 0, 0);
        requestQueue.add(new RequestInfo(null, receiveBuffer, null, dataR));
      }

      while (requestQueue.size() > 0) {
        RequestInfo receiveRequest = requestQueue.peek();
        Status status = receiveRequest.request.testStatus();
        if (receiveRequest != null && receiveRequest.request != null && status != null) {
          RequestInfo info = requestQueue.poll();
          ByteBuffer receiveBuffer = info.recvBuffer;
          completed++;
          int count = status.getCount(MPI.BYTE);
//          LOG.info("Received message with size: " + count);
          receiveBuffer.position(count);
          receiveBuffer.flip();
          receiveBuffer.get(receiveBytes);
          int c = receiveBuffer.getInt(0);
//          LOG.info("Completed: " + c);
          recvBuffers.add(receiveBuffer);
        } else{
          break;
        }
      }
    }
  }
}
