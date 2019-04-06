package edu.iu.dsc.tws.mpiapps.cols;

import edu.iu.dsc.tws.mpiapps.Collective;
import mpi.MPI;
import mpi.MPIException;

import java.nio.IntBuffer;

public class Reduce extends Collective {

    private int rank;

    private int[] values;

    private IntBuffer sendBuffer;
    private IntBuffer receiveBuffer;


    public Reduce(int size, int iterations) {
        super(size, iterations);
        values = new int[size];
        sendBuffer = MPI.newIntBuffer(size);
        receiveBuffer = MPI.newIntBuffer(size);
        try {
            rank = MPI.COMM_WORLD.getRank();
        } catch (MPIException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute() throws MPIException {
        for (int i = 0; i < iterations; i++) {
            MPI.COMM_WORLD.reduce(sendBuffer, receiveBuffer, size, MPI.INT, MPI.SUM, 0);
            MPI.COMM_WORLD.barrier();
            receiveBuffer.clear();
        }
    }
}
