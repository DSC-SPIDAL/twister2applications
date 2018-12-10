package edu.iu.dsc.tws.apps.kmeans;

public class KMeansCenters {
    private double[][] centers;
    private int[] centerSums;

    public KMeansCenters() {
    }

    public KMeansCenters(double[][] centerValues) {
        this.centers = centerValues;
    }

    public KMeansCenters(double[][] centerValues, int[] centerSums) {
        this.centers = centerValues;
        this.centerSums = centerSums;
    }

    public double[][] getCenters() {
        return centers;
    }

    public KMeansCenters setCenters(double[][] centerValues) {
        this.centers = centerValues;
        return this;
    }

    public int[] getCenterSums() {
        return centerSums;
    }

    public KMeansCenters setCenterSums(int[] cSums) {
        this.centerSums = cSums;
        return this;
    }
}
