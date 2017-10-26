package edu.iu.dsc.tws.mpiapps;

import mpi.Intercomm;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

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

    }

    public static void tearDownParallelism() throws MPIException {
        // End MPI
        MPI.Finalize();
    }
}
