package edu.iu.dsc.tws.mpiapps;

import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class Reduce extends Collective {
  private RandomString randomString;

  private KryoSerializer kryoSerializer;

  public Reduce(int size, int iterations) {
    super(size, iterations);
    this.randomString = new RandomString(size);
    this.kryoSerializer = new KryoSerializer();
  }

  @Override
  public void execute() throws MPIException {
    IntBuffer intBuffer = MPI.newIntBuffer(size);
    IntBuffer receiveBuffer = MPI.newIntBuffer(size);

    IntBuffer maxSend = MPI.newIntBuffer(1);
    IntBuffer minSend = MPI.newIntBuffer(1);

    for (int i = 0; i < size; i++) {
      intBuffer.put(i);
    }

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
        byteBuffer.get(secondBytes);


      }
    }, true);

    for (int i = 0; i < iterations; i++) {
      String next = randomString.nextRandomSizeString();
      byte[]stringBytes = next.getBytes();
      MPI.COMM_WORLD.reduce(intBuffer, receiveBuffer, size, MPI.BYTE, o, 0);
    }
  }
}
