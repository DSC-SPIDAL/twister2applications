package edu.iu.dsc.tws.spark.terasort;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class ByteOutputFormat implements OutputFormat<byte[], byte[]> {

    @Override
    public org.apache.hadoop.mapred.RecordWriter<byte[], byte[]> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        return new EmptyRecordWriter();
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
}
