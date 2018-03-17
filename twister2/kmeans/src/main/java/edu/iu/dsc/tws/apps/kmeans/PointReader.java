package edu.iu.dsc.tws.apps.kmeans;

import java.io.*;

public class PointReader {
  public static double[][] readPoints(String fileName, int noOfPoints, int noOfProcs,
                               int procIndex, int noOfTasks, int dimension) throws IOException {
    int pointsPerTask = noOfPoints / (noOfProcs * noOfTasks);
    int offset = pointsPerTask * procIndex;
    double[][] doubles = new double[noOfTasks][];
    for (int i = 0; i < noOfTasks; i++) {
      doubles[i] = new double[pointsPerTask * dimension];
    }

    File f = new File(fileName);
    BufferedReader b = new BufferedReader(new FileReader(f));
    String readLine = "";

    System.out.println("Reading file using Buffered Reader");

    int noOfRecords = 0;
    int currentTask = 0;
    int currentRecordsPerTask = 0;
    while ((readLine = b.readLine()) != null) {
      noOfRecords++;
      if (noOfRecords < offset) {
        continue;
      }

      String[] split = readLine.split(" ");
      if (split.length != dimension) {
        throw new RuntimeException("Invalid line with length: " + split.length);
      }

      for (int i = 0; i < dimension; i++) {
        doubles[currentTask][i + currentTask * dimension] = Double.parseDouble(split[i]);
      }
      currentRecordsPerTask++;

      if (currentRecordsPerTask == pointsPerTask) {
        currentRecordsPerTask = 0;
        currentTask++;
      }
    }
    return doubles;
  }

  public static double[] readClusters(String clusterFileName,  int dimension, int clusters) throws IOException {
    double[] clusterPoints = new double[dimension * clusters];

    File f = new File(clusterFileName);
    BufferedReader b = new BufferedReader(new FileReader(f));
    String readLine = "";

    System.out.println("Reading file using Buffered Reader");

    int noOfRecords = 0;
    while ((readLine = b.readLine()) != null) {

      String[] split = readLine.split(" ");
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
