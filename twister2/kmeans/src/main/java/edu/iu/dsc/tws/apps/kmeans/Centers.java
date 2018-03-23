package edu.iu.dsc.tws.apps.kmeans;

public class Centers {
  private double[] centers;

  private int[] centerSums;

  public Centers(double[] centers, int[] centerSums) {
    this.centers = centers;
    this.centerSums = centerSums;
  }

  public void setCenters(double[] centers) {
    this.centers = centers;
  }

  public void setCenterSums(int[] centerSums) {
    this.centerSums = centerSums;
  }

  public double[] getCenters() {
    return centers;
  }

  public int[] getCenterSums() {
    return centerSums;
  }

  public Centers() {
  }
}
