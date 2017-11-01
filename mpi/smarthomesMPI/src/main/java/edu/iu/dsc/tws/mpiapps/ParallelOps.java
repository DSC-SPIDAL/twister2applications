package edu.iu.dsc.tws.mpiapps;

import mpi.*;

import java.nio.DoubleBuffer;

/**
 * Created by pulasthi on 10/23/17.
 */
public class ParallelOps {
    public static int nodeCount=1;
    public static int threadCount=1;
    public static String machineName;
    public static Intracomm worldProcsComm;
    public static int worldProcRank;
    public static int worldProcsCount;
    public static int worldProcsPerNode;
    public static int plugNumStart;
    public static int plugNumAssigned;
    public static int assignedPlugs[];

    public static DoubleBuffer doubleBuffer;

    public static void setupParallelism(String[] args) throws MPIException {
        MPI.Init(args);
        machineName = MPI.getProcessorName();
        worldProcsComm = MPI.COMM_WORLD; //initializing MPI world communicator
        worldProcRank = worldProcsComm.getRank();
        worldProcsCount = worldProcsComm.getSize();
//        Intracomm newsmall = new Intracomm()
        assignedPlugs = new int[worldProcsCount];
        /* Create communicating groups */
        worldProcsPerNode = worldProcsCount / nodeCount;
        boolean heterogeneous = (worldProcsPerNode * nodeCount) != worldProcsCount;
        if (heterogeneous) {
            Utils.printMessage("Running in heterogeneous mode");
        }

        // calculating plug ids assinged to each process
        int q,r;
        q = Math.floorDiv(SmartHomesDriver.config.numPlugs,worldProcsCount);
        r = SmartHomesDriver.config.numPlugs%worldProcsCount;
        plugNumAssigned = q;
        plugNumStart = worldProcRank*q;
        if(worldProcRank < r){
            plugNumAssigned = q + 1;
            plugNumStart = worldProcRank*(q + 1);
        }
        for (int i = 0; i < worldProcsCount; i++) {
            String arg = args[i];
            q = Math.floorDiv(SmartHomesDriver.config.numPlugs,worldProcsCount);
            r = SmartHomesDriver.config.numPlugs%worldProcsCount;
            assignedPlugs[i] = q;
            if(i < r){
                assignedPlugs[i] = q + 1;
            }
        }
//        doubleBuffer = MPI.newDoubleBuffer(SmartHomesDriver.config.numHouses*SmartHomesDriver.basicSliceCount);
        doubleBuffer = MPI.newDoubleBuffer(40*1440);

    }

    public static void tearDownParallelism() throws MPIException {
        // End MPI
        MPI.Finalize();
    }

    public static  void allReduce(double [] values, Op reduceOp) throws MPIException{
        allReduce(values, reduceOp, worldProcsComm);
    }

    public static void allReduce(double [] values, Op reduceOp, Intracomm comm) throws MPIException {
        comm.allReduce(values, values.length, MPI.DOUBLE, reduceOp);
    }

    public static  void allReduceBuff(double [] values, Op reduceOp, double[] result) throws MPIException{
        allReduceBuff(values, reduceOp, worldProcsComm, result);
    }

    public static void allReduceBuff(double [] values, Op reduceOp, Intracomm comm,  double[] result) throws MPIException {
        doubleBuffer.clear();
        doubleBuffer.put(values,0,values.length);
        comm.allReduce(doubleBuffer, values.length, MPI.DOUBLE, reduceOp);
        doubleBuffer.flip();
        doubleBuffer.get(result);
    }
}
