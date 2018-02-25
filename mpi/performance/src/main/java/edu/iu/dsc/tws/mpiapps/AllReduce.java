package edu.iu.dsc.tws.mpiapps;

import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;

public class AllReduce extends Collective {
  private RandomString randomString;

  private KryoSerializer kryoSerializer;

  private long allReduceTime = 0;

  private long reduceTime = 0;

  public AllReduce(int size, int iterations) {
    super(size, iterations);
    this.randomString = new RandomString(size);
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
      long start = 0;
      maxSend.put(0, bytes.length);
      start = System.nanoTime();
      MPI.COMM_WORLD.allReduce(maxSend, maxRecv, 1, MPI.INT, MPI.MAX);
      reduceTime += System.nanoTime() - start;
      int length = maxRecv.get(0) + 4;

      Datatype stringBytes = Datatype.createContiguous(length, MPI.BYTE);
      stringBytes.commit();

      sendBuffer.clear();
      sendBuffer.putInt(bytes.length);
      sendBuffer.put(bytes);
      start = System.nanoTime();
      MPI.COMM_WORLD.allReduce(sendBuffer, receiveBuffer, 1, stringBytes, reduceOp());
      allReduceTime += System.nanoTime() - start;

      if (rank == 0) {
        int receiveLength = receiveBuffer.getInt(0);
        byte[] receiveBytes = new byte[receiveLength];
        receiveBuffer.position(receiveLength + 4);
        receiveBuffer.flip();
        receiveBuffer.getInt();
        receiveBuffer.get(receiveBytes);
        String rcv = (String) kryoSerializer.deserialize(receiveBytes);
      }
      receiveBuffer.clear();
      sendBuffer.clear();
      maxRecv.clear();
      maxSend.clear();
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

        String firstString = (String) kryoSerializer.deserialize(firstBytes);
        String secondString = (String) kryoSerializer.deserialize(secondBytes);
        secondBytes = kryoSerializer.serialize(secondString);

        inOut.clear();
        inOut.putInt(secondBytes.length);
        inOut.put(secondBytes);
      }
    }, true);
  }
}
