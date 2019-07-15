package edu.iu.dsc.tws.spark.terasort;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteInputSplit extends InputSplit implements Writable {
    private int elements = 10000000;

    private String node;

    @Override
    public long getLength() throws IOException, InterruptedException {
        return elements * 100;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        String[] ret = new String[1];
        ret[0] = node;
        return ret;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
