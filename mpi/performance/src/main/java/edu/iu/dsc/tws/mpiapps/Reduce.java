package edu.iu.dsc.tws.mpiapps;

import mpi.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class Reduce extends Collective {
  public Reduce(int size, int iterations) {
    super(size, iterations);
  }

  @Override
  public void execute() throws MPIException {
    IntBuffer intBuffer = MPI.newIntBuffer(size);
    IntBuffer receiveBuffer = MPI.newIntBuffer(size);
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

        
      }
    }, true);

    for (int i = 0; i < iterations; i++) {
      Datatype type = Datatype.createContiguous(1, MPI.BYTE);
      type.commit();
      MPI.COMM_WORLD.reduce(intBuffer, receiveBuffer, size, type, MPI.SUM, 0);
    }
  }
}
