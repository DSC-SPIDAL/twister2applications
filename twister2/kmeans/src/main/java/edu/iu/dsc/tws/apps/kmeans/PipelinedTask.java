package edu.iu.dsc.tws.apps.kmeans;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.logging.Logger;

public class PipelinedTask {
  private static final Logger LOG = Logger.getLogger(PipelinedTask.class.getName());

  private int currentIteration;

  private double[] points;

  private double[] centers;

  private double[] centerSums;

  private int[] centerCounts;

  private int taskId;

  private int dimension;

  private DataFlowOperation allReduce;

  private int noOfIterations;

  private int pointsForThread;

  private List<Long> emitTimes = new ArrayList<>();

  private List<Double> computeTimes = new ArrayList<>();

  public PipelinedTask(double[] points, double[] centers, int taskId, int dimension, int noOfIterations, int pointsForThread) {
    this.points = points;
    this.centers = centers;
    this.taskId = taskId;
    this.dimension = dimension;
    this.noOfIterations = noOfIterations;
    this.pointsForThread = pointsForThread;

    this.centerSums = new double[centers.length];
    this.centerCounts = new int[centers.length / dimension];
  }

  public void setAllReduce(DataFlowOperation allReduce) {
    this.allReduce = allReduce;
  }

  public boolean executeMap() {
    if (currentIteration >= noOfIterations) {
      LOG.info("Done iterations");
      return false;
    }

    long start = System.nanoTime();
//    LOG.info(String.format("%d Points per thread %d itr %d", taskId,  pointsForThread, currentIteration));
    findNearesetCenters(dimension, points, centers, centerSums, pointsForThread);
    currentIteration++;
    double time = (System.nanoTime() - start) / 1000000.0;
    computeTimes.add(time);

    // now communicate
    emitTimes.add(System.currentTimeMillis());
//    LOG.info(String.format("%d Sending centersum with length %d", taskId, centerSums.length));
    allReduce.send(taskId, new Centers(centerSums, centerCounts), 0);

    return true;
  }

  public List<Double> getComputeTimes() {
    return computeTimes;
  }

  public List<Long> getEmitTimes() {
    return emitTimes;
  }

  public void updateCenters(Centers newCenters) {
    if (centers.length != newCenters.getCenters().length) {
      throw new RuntimeException(String.format("%d Received new centers with length %d", taskId, newCenters.getCenters().length));
    }
    Arrays.fill(centerCounts, 0);
    centers = newCenters.getCenters();
  }

  public void progress() {
    allReduce.progress();
  }

  private void findNearesetCenters(int dimension, double[] points, double[] centers,
                                          double[] centerSumsAndCountsForThread,
                                          int pointsForThread) {
    for (int i = 0; i < pointsForThread; ++i) {
      int centerWithMinDist = findCenterWithMinDistance(points, centers, dimension, i);
      int centerOffset = centerWithMinDist * dimension;
      accumulate(points, centerSumsAndCountsForThread, i, centerOffset, dimension);
      centerSums[centerWithMinDist]++;
    }
  }

  private int findCenterWithMinDistance(double[] points, double[] centers, int dimension, int pointOffset) {
    int k = centers.length / dimension;
    double dMin = Double.MAX_VALUE;
    int dMinIdx = -1;
//    LOG.info(String.format("%d K %d", taskId, k));
    for (int j = 0; j < k; ++j) {
      double dist = getEuclideanDistance(points, centers, dimension, pointOffset, j * dimension);
      if (dist < dMin) {
        dMin = dist;
        dMinIdx = j;
      }
    }
    return dMinIdx;
  }

  private void accumulate(double[] points, double[] centerSumsAndCounts,
                                 int pointOffset, int centerOffset, int dimension) {
    for (int i = 0; i < dimension; ++i) {
      centerSumsAndCounts[centerOffset + i] += points[pointOffset + i];
    }
  }

  private double getEuclideanDistance(double[] point1, double[] point2,
                                             int dimension, int point1Offset, int point2Offset) {
    double d = 0.0;
    for (int i = 0; i < dimension; ++i) {
      d += Math.pow(point1[i + point1Offset] - point2[i + point2Offset], 2);
    }
    return Math.sqrt(d);
  }
}
