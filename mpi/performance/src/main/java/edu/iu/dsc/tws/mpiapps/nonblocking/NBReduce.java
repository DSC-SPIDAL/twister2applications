package edu.iu.dsc.tws.mpiapps.nonblocking;

import edu.iu.dsc.tws.mpiapps.Collective;
import edu.iu.dsc.tws.mpiapps.DataGenUtils;
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
    Object next = DataGenUtils.generateData(size);

    int maxPending = 16;
    Queue<ByteBuffer> sendBuffers = new ArrayBlockingQueue<>(maxPending);
    Queue<ByteBuffer> recvBuffers = new ArrayBlockingQueue<>(maxPending);

    Queue<RequestInfo> receiveRequestQueue = new ArrayBlockingQueue<>(maxPending);

    for (int i = 0; i < maxPending; i++) {
      sendBuffers.add(MPI.newByteBuffer(size * 5));
      recvBuffers.add(MPI.newByteBuffer(size * 5));
    }

    int completed = 0;
    while (completed < iterations) {
      if (receiveRequestQueue.size() < maxPending) {
        byte[] bytes = kryoSerializer.serialize(next);

        ByteBuffer sendBuffer = sendBuffers.poll();
        ByteBuffer receiveBuffer = recvBuffers.poll();

        if (sendBuffer == null || receiveBuffer == null) {
          throw new RuntimeException("Buffer null");
        }

        sendBuffer.putInt(bytes.length);
        sendBuffer.put(bytes);
        Datatype stringBytes = Datatype.createContiguous(bytes.length + 4, MPI.BYTE);
        stringBytes.commit();

        System.out.println("send: " + bytes.length);
        Request dataR = MPI.COMM_WORLD.iReduce(sendBuffer, receiveBuffer, 1, stringBytes, reduceOp(), 0);
        receiveRequestQueue.add(new RequestInfo(sendBuffer, receiveBuffer, bytes, dataR));
      }

      RequestInfo receiveRequest = receiveRequestQueue.peek();
      if (receiveRequest.request.testStatus() != null) {
        RequestInfo info = receiveRequestQueue.poll();

        ByteBuffer sendBuffer = info.sendBuffer;
        ByteBuffer receiveBuffer = info.recvBuffer;

        sendBuffer.clear();
        if (rank == 0) {
          int receiveLength = receiveBuffer.getInt(0);
          System.out.println("Receve: " + receiveLength);
          byte[] receiveBytes = new byte[receiveLength];
          receiveBuffer.position(receiveLength + 4);
          receiveBuffer.flip();
          receiveBuffer.getInt();
          receiveBuffer.get(receiveBytes);
          IntData rcv = (IntData) kryoSerializer.deserialize(receiveBytes);
        }
        receiveBuffer.clear();
        sendBuffer.clear();
        sendBuffers.add(sendBuffer);
        recvBuffers.add(receiveBuffer);

        completed++;
      }
    }

    if (rank == 0) {
      System.out.println("Final time: " + allReduceTime / 1000000 + " ," + reduceTime / 1000000);
    }
  }

  private Op reduceOp() {
    return new Op(new UserFunction() {
      @Override
      public void call(Object o, Object o1, int i, Datatype datatype) throws MPIException {
        super.call(o, o1, i, datatype);
      }

      @Override
      public void call(ByteBuffer in, ByteBuffer inOut, int i, Datatype datatype) throws MPIException {
        int length1 = in.getInt();
        int length2 = inOut.getInt();
        byte[] firstBytes = new byte[length1];
        byte[] secondBytes = new byte[length2];

        in.get(firstBytes);
        inOut.get(secondBytes);

        IntData firstString = (IntData) kryoSerializer.deserialize(firstBytes);
        IntData secondString = (IntData) kryoSerializer.deserialize(secondBytes);
        secondBytes = kryoSerializer.serialize(secondString);

        inOut.clear();
        inOut.putInt(secondBytes.length);
        inOut.put(secondBytes);
      }
    }, true);
  }
}
