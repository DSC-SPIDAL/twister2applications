package edu.iu.dsc.tws.apps.terasort.utils;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Data partitioner reads data from x number of nodes each with y number of records.
 * These records are sorted and we pick n number of partition keys from this final sorted array
 */
public class DataPartitioner {
    private static Logger LOG = Logger.getLogger(DataPartitioner.class.getName());
    // number of records to read from each place
    private int numberOfRecords;
    DataFlowOperation gather;

    // the global rank
    private int globalRank;

    // total number of processes in the system
    private int worldSize;

    // rank specific to communicator

    public DataPartitioner(DataFlowOperation samplesGather, int partitionSamplesPerNode, int size) {
        numberOfRecords = partitionSamplesPerNode;
        gather = samplesGather;
        worldSize = size;
    }

    /**
     * The records used by this partitioner
     *
     * @param records total number of records
     */
    public byte[] execute(Record[] records) {
        if (records.length != numberOfRecords) {
            String msg = "Records length should be equal to 0 or " + numberOfRecords;
            LOG.log(Level.SEVERE, msg);
            throw new RuntimeException(msg);
        }

        int noOfSelectedKeys = worldSize - 1;
        byte[] selectedKeys = new byte[Record.KEY_SIZE * noOfSelectedKeys];


//        if (globalRank < places) {
//            int totalRecordsSize = Record.KEY_SIZE * numberOfRecords * places;
//            byte[] totalRecords = new byte[totalRecordsSize];
//            int bytestToSend = numberOfRecords * Record.KEY_SIZE;
//            byte[] sendingKeys = new byte[bytestToSend];
//            LOG.log(Level.INFO, "Rank: " + globalRank + " Local: " + rank + " Total record: " +
//                    totalRecordsSize + " process records: " + numberOfRecords + " Sending bytes: " + bytestToSend);
//            for (int i = 0; i < numberOfRecords; i++) {
//                Record r = records[i];
//                System.arraycopy(r.getKey().getBytes(), 0, sendingKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);
//            }
//            // send the records
////      partitionCommunicator.gather(sendingKeys, bytestToSend, MPI.BYTE, totalRecords, bytestToSend, MPI.BYTE, 0);
//
//            // now sort the keys
//            if (globalRank == 0) {
//                Record[] partitionRecords = new Record[numberOfRecords * places];
//                for (int i = 0; i < numberOfRecords * places; i++) {
//                    byte[] key = new byte[Record.KEY_SIZE];
//                    System.arraycopy(totalRecords, i * Record.KEY_SIZE, key, 0, Record.KEY_SIZE);
//                    partitionRecords[i] = new Record(new Text(key));
//                }
//
//                Arrays.sort(partitionRecords);
//
//                int div = numberOfRecords * places / worldSize;
//                for (int i = 0; i < noOfSelectedKeys; i++) {
//                    System.arraycopy(partitionRecords[(i + 1) * div].getKey().getBytes(), 0,
//                            selectedKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);
//                }
//            }
//        }

        // process 0 has the records, send them to all the nodes
        // we broadcast this to all the nodes
//    MPI.COMM_WORLD.bcast(selectedKeys, Record.KEY_SIZE * noOfSelectedKeys, MPI.BYTE, 0);
        return selectedKeys;
    }
}
