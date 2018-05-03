package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import java.io.Serializable;

public class LaserScanMessage implements Serializable {
    private long timestamp;

    private double range_max;

    private double range_min;

    private double angle_min;

    private double angle_max;

    private double angle_increment;

    private double[] ranges;

    public long getTimestamp() {
        return timestamp;
    }

    public double getRange_max() {
        return range_max;
    }

    public double getRange_min() {
        return range_min;
    }

    public double getAngle_min() {
        return angle_min;
    }

    public double getAngle_max() {
        return angle_max;
    }

    public double getAngle_increment() {
        return angle_increment;
    }

    public double[] getRanges() {
        return ranges;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setRange_max(double range_max) {
        this.range_max = range_max;
    }

    public void setRange_min(double range_min) {
        this.range_min = range_min;
    }

    public void setAngle_min(double angle_min) {
        this.angle_min = angle_min;
    }

    public void setAngle_max(double angle_max) {
        this.angle_max = angle_max;
    }

    public void setAngle_increment(double angle_increment) {
        this.angle_increment = angle_increment;
    }

    public void setRanges(double[] ranges) {
        this.ranges = ranges;
    }
}
