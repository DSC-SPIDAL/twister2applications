package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SlamDataReader {
  private String file;

  private BufferedReader br;

  public SlamDataReader(String file) {
    this.file = file;

    try {
      br = new BufferedReader(new FileReader(file));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public LaserScan read() {
    String line = null;
    try {
      line = br.readLine();
      while (line != null) {
        String array[] = line.split(" ");
        if (array != null && array.length > 0) {
          if (array[0].equals("FLASER")) {
            return createFromList(array);
          }
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private LaserScan createFromList(String []array) {
    int noOfScans = Integer.parseInt(array[1]);
    LaserScan laserScan = new LaserScan();
    laserScan.setAngleIncrement(Math.PI / noOfScans);
    laserScan.setTimestamp(System.currentTimeMillis());

    laserScan.setAngleMax(Math.PI / 2);
    laserScan.setAngleMin(-Math.PI / 2);

    List<Double> ranges = new ArrayList<Double>(noOfScans);
    int i = 2;
    for (; i < 2 + noOfScans; i++) {
      ranges.add(Double.parseDouble(array[i]));
    }
    laserScan.setRanges(ranges);
    laserScan.setRangeMax(50.0);
    laserScan.setRangeMin(.25);

    i += 3;
    DoubleOrientedPoint pose = new DoubleOrientedPoint(Double.parseDouble(array[i]),
        Double.parseDouble(array[i + 1]), Double.parseDouble(array[i + 2]));
    laserScan.setPose(pose);
    return laserScan;
  }
}
