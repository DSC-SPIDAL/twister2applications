package edu.iu.dsc.comms.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CSVWriter {

    public static void write(String filePath, JobParameters jobParameters, long time) throws IOException {
        File file = new File(filePath);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        boolean isNewFile = file.exists();

        BufferedWriter br = new BufferedWriter(new FileWriter(file, true));

        if (!isNewFile) {
            br.write("operation,iterations,containers,stages,size,time(ms)");
            br.newLine();
        }

        String row = jobParameters.getOperation() +
                ',' +
                jobParameters.getIterations() +
                ',' +
                jobParameters.getContainers() +
                ',' +
                jobParameters.getTaskStages().toString() +
                ',' +
                jobParameters.getSize() +
                ',' +
                time;
        br.write(row);
        br.newLine();
        br.close();
    }
}
