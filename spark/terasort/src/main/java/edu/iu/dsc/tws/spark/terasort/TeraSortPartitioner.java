package edu.iu.dsc.tws.spark.terasort;

import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.*;

public class TeraSortPartitioner extends Partitioner implements Serializable {
    @Override
    public int getPartition(Object key) {
        if (!initialized) {
            Set<Integer> h = new HashSet<>();
            for (int j = 0; j < 10; j++) {
                h.add(j);
            }
            prepare(h);
            initialized = true;
        }

        return partition((byte[]) key);
    }

    private int keysToOneTask;

    private List<Integer> destinationsList;

    private boolean initialized = false;

    void prepare(Set<Integer> destinations) {
        int totalPossibilities = 256 * 256; //considering only most significant bytes of array
        this.keysToOneTask = (int) Math.ceil(totalPossibilities / (double) destinations.size());
        this.destinationsList = new ArrayList<>(destinations);
        Collections.sort(this.destinationsList);
    }

    int getIndex(byte[] array) {
        int key = ((array[0] & 0xff) << 8) + (array[1] & 0xff);
        return key / keysToOneTask;
    }

    int partition(byte[] data) {
        Integer integer = this.destinationsList.get(this.getIndex(data));
        return integer;
    }

    @Override
    public int numPartitions() {
        return 10;
    }
}
