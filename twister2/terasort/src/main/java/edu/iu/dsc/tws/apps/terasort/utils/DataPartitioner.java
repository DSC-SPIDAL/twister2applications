package edu.iu.dsc.tws.apps.terasort.utils;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    public byte[] execute(List<Record> records) {
        int noOfSelectedKeys = worldSize - 1;
        byte[] selectedKeys = new byte[Record.KEY_SIZE * noOfSelectedKeys];
        //Sort the collected records
        Collections.sort(records);
        int div = records.size() / worldSize;
        for (int i = 0; i < noOfSelectedKeys; i++) {
            System.arraycopy(records.get((i + 1) * div).getKey().getBytes(), 0,
                            selectedKeys, i * Record.KEY_SIZE, Record.KEY_SIZE);
        }
        return selectedKeys;
    }
}
