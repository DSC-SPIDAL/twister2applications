package edu.iu.dsc.tws.mpiapps;

import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;

public class Reduce extends Collective {
  private RandomString randomString;

  private KryoSerializer kryoSerializer;

  public Reduce(int size, int iterations) {
    super(size, iterations);
    this.randomString = new RandomString(size / 2);
    this.kryoSerializer = new KryoSerializer();
    this.kryoSerializer.init(new HashMap());
  }

  @Override
  public void execute() throws MPIException {
    IntBuffer maxSend = MPI.newIntBuffer(1);
    IntBuffer maxRecv = MPI.newIntBuffer(1);
    int rank = MPI.COMM_WORLD.getRank();
    ByteBuffer sendBuffer = MPI.newByteBuffer(size * 2);
    ByteBuffer receiveBuffer = MPI.newByteBuffer(size * 2);

    for (int i = 0; i < iterations; i++) {
      String next = randomString.nextString();
      byte[] bytes = kryoSerializer.serialize(next);
      maxSend.put(0, bytes.length);
      MPI.COMM_WORLD.allReduce(maxSend, maxRecv, 1, MPI.INT, MPI.MAX);
      int length = maxRecv.get(0) + 4;

      sendBuffer.clear();
      sendBuffer.putInt(bytes.length);
      sendBuffer.put(bytes);
      MPI.COMM_WORLD.reduce(sendBuffer, receiveBuffer, length, MPI.BYTE, reduceOp(), 0);

      if (rank == 0) {
        int receiveLength = receiveBuffer.getInt(0);
        byte[] receiveBytes = new byte[receiveLength];
        receiveBuffer.position(receiveLength + 4);
        receiveBuffer.flip();
        receiveBuffer.getInt();
        receiveBuffer.get(receiveBytes);
        String rcv = (String) kryoSerializer.deserialize(receiveBytes);
//        System.out.println(rcv);
      }
      receiveBuffer.clear();
      sendBuffer.clear();
      maxRecv.clear();
      maxSend.clear();
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

        System.out.println(String.format("Partial:%d %d %d %d %d %d %d %d", inOut.position(),
        inOut.capacity(), inOut.limit(), length2, in.position(), in.capacity(), in.limit(), length1));
        in.get(firstBytes);
        inOut.get(secondBytes);

        String firstString = (String) kryoSerializer.deserialize(firstBytes);
        String secondString = (String) kryoSerializer.deserialize(secondBytes);
        secondBytes = kryoSerializer.serialize(secondString);

        inOut.clear();
        inOut.putInt(secondBytes.length);
        inOut.put(secondBytes);
      }
    }, false);
  }
}
