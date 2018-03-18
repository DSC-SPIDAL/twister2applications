package edu.iu.dsc.tws.apps.kmeans;

import java.io.*;
import java.util.logging.Logger;

public class PointReader {
  private static final Logger LOG = Logger.getLogger(PointReader.class.getName());

  public static double[][] readPoints(String fileName, int noOfPoints, int noOfProcs,
                               int procIndex, int taskPerProc, int dimension) throws IOException {
    int pointsPerTask = noOfPoints / (taskPerProc * noOfProcs);
    int offset = pointsPerTask * procIndex;
    double[][] doubles = new double[taskPerProc][];
    for (int i = 0; i < taskPerProc; i++) {
      doubles[i] = new double[pointsPerTask * dimension];
    }

    File f = new File(fileName);
    BufferedReader b = new BufferedReader(new FileReader(f));
    String readLine = "";

    int noOfRecords = 0;
    int currentTask = 0;
    int currentRecordsPerTask = 0;
    int maxRecordsToRead = offset + noOfPoints / noOfProcs;
    while ((readLine = b.readLine()) != null) {
      noOfRecords++;
      if (noOfRecords < offset) {
        continue;
      }

      if (noOfRecords >= maxRecordsToRead) {
        break;
      }

      String[] split = readLine.split(",");
      if (split.length != dimension) {
        throw new RuntimeException("Invalid line with length: " + split.length);
      }

//      LOG.info(String.format("ofset %d points per task %d records %d records per task %d current task %d",
//          offset, pointsPerTask, noOfRecords, currentRecordsPerTask, currentTask));
      for (int i = 0; i < dimension; i++) {
        doubles[currentTask][i + currentTask * dimension] = Double.parseDouble(split[i].trim());
      }
      currentRecordsPerTask++;

      if (currentRecordsPerTask == pointsPerTask) {
        currentRecordsPerTask = 0;
        currentTask++;
        if (currentTask == taskPerProc) {
          break;
        }
      }
    }
    return doubles;
  }

  public static double[] readClusters(String clusterFileName,  int dimension, int clusters) throws IOException {
    double[] clusterPoints = new double[dimension * clusters];

    File f = new File(clusterFileName);
    BufferedReader b = new BufferedReader(new FileReader(f));
    String readLine = "";

    int noOfRecords = 0;
    while ((readLine = b.readLine()) != null) {

      String[] split = readLine.split(",");
      if (split.length != dimension) {
        throw new RuntimeException("Invalid line with length: " + split.length);
      }

      for (int i = 0; i < dimension; i++) {
        clusterPoints[noOfRecords * dimension + i] = Double.parseDouble(split[i]);
      }
      noOfRecords++;
    }

    if (noOfRecords < clusters) {
      throw new RuntimeException("No of clusters not equal: " + noOfRecords);
    }

    return clusterPoints;
  }
}
