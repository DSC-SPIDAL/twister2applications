package edu.iu.dsc.tws.apps.slam.utils;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;

import java.io.*;

public class FileIO {
    private PrintWriter pw;

    private BufferedReader br;

    public FileIO(String file, boolean write) {
        try {
            if (write) {
                pw = new PrintWriter(new FileWriter(file));
            } else {
                br = new BufferedReader(new FileReader(file));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(LaserScan laserScan) {
        String s = laserScan.getString();
        pw.printf("%s\n", s);
        pw.flush();
    }

    public void writeResult(String value) {
        pw.printf("%s\n", value);
        pw.flush();
    }

    public LaserScan read() {
        String line = null;
        try {
            line = br.readLine();
            if (line != null) {
                LaserScan laserScan = new LaserScan();
                laserScan.loadFromString(line);
                return laserScan;
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
