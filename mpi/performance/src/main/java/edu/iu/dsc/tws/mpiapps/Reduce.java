package edu.iu.dsc.tws.mpiapps;

import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
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
    ByteBuffer sendBuffer = MPI.newByteBuffer(size * 2);
    ByteBuffer receiveBuffer = MPI.newByteBuffer(size * 2);

    IntBuffer maxSend = MPI.newIntBuffer(1);
    IntBuffer minSend = MPI.newIntBuffer(1);

    Op o = new Op(new UserFunction() {
      @Override
      public void call(Object o, Object o1, int i, Datatype datatype) throws MPIException {
        super.call(o, o1, i, datatype);
      }

      @Override
      public void call(ByteBuffer byteBuffer, ByteBuffer byteBuffer1, int i, Datatype datatype) throws MPIException {
        int length1 = byteBuffer.getInt();
        int length2 = byteBuffer1.getInt();

        byte[] firstBytes = new byte[length1];
        byte[] secondBytes = new byte[length2];

        byteBuffer.get(firstBytes);
        byteBuffer1.get(secondBytes);

        String firstString = (String) kryoSerializer.deserialize(firstBytes);
        String secondString = (String) kryoSerializer.deserialize(secondBytes);
        System.out.println("partial: " + firstString + ", " + secondString);

//        byteBuffer1.clear();
//        byteBuffer1.putInt(secondBytes.length);
//        byteBuffer1.put(secondBytes);
      }
    }, true);

    for (int i = 0; i < iterations; i++) {
      String next = randomString.nextString();
//      String next = "12345";
      byte[] bytes = kryoSerializer.serialize(next);
      System.out.println("Length: " + bytes.length + " out: " + next);
      sendBuffer.clear();
      sendBuffer.putInt(bytes.length);
      sendBuffer.put(bytes);
      MPI.COMM_WORLD.reduce(sendBuffer, receiveBuffer, size, MPI.BYTE, o, 0);

      int receiveLength = receiveBuffer.getInt(0);
      System.out.println("receive length: " + receiveLength);
      byte[] receiveBytes = new byte[receiveLength];
      receiveBuffer.position(receiveLength + 4);
      receiveBuffer.flip();
      receiveBuffer.getInt();
      receiveBuffer.get(receiveBytes);
      String rcv = (String) kryoSerializer.deserialize(receiveBytes);
      System.out.println(rcv);
    }
  }
}
