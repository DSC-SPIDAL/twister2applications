package edu.iu.dsc.tws.spark.terasort;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteInputSplit extends InputSplit implements Writable {
    private int elements = 10000;

    @Override
    public long getLength() throws IOException, InterruptedException {
        return elements * 100;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
