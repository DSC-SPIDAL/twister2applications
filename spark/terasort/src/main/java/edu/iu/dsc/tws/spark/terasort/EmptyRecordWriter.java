package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class EmptyRecordWriter implements RecordWriter<byte[], byte[]> {
    @Override
    public void write(byte[] bytes, byte[] bytes2) throws IOException {

    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }

}
