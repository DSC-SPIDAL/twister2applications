package edu.iu.dsc.tws.apps.slam.core.app;

import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

import java.util.ArrayList;
import java.util.List;

public class LaserScan {
    protected long timestamp;

    protected double rangeMax;

    protected double rangeMin;

    protected double angleMin;

    protected double angleMax;

    protected double angleIncrement;

    protected List<Double> ranges = new ArrayList<Double>();

    private DoubleOrientedPoint pose;

    public DoubleOrientedPoint getPose() {
        return pose;
    }

    public void setPose(DoubleOrientedPoint pose) {
        this.pose = pose;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getRangeMax() {
        return rangeMax;
    }

    public double getRangeMin() {
        return rangeMin;
    }

    public double getAngleMin() {
        return angleMin;
    }

    public double getAngleMax() {
        return angleMax;
    }

    public double getAngleIncrement() {
        return angleIncrement;
    }

    public List<Double> getRanges() {
        return ranges;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setRangeMax(double rangeMax) {
        this.rangeMax = rangeMax;
    }

    public void setRangeMin(double rangeMin) {
        this.rangeMin = rangeMin;
    }

    public void setAngleMin(double angleMin) {
        this.angleMin = angleMin;
    }

    public void setAngleMax(double angleMax) {
        this.angleMax = angleMax;
    }

    public void setAngleIncrement(double angleIncrement) {
        this.angleIncrement = angleIncrement;
    }

    public void setRanges(List<Double> ranges) {
        this.ranges = ranges;
    }

    public String getString() {
        StringBuilder builder = new StringBuilder();
        builder.append(rangeMax).append(",");
        builder.append(rangeMin).append(",");
        builder.append(angleMin).append(",");
        builder.append(angleMax).append(",");
        builder.append(angleIncrement).append(",");
        builder.append(pose.getX()).append(",");
        builder.append(pose.getY()).append(",");
        builder.append(pose.getTheta());
        for (Double range : ranges) {
            builder.append(",").append(range);

        }
        return builder.toString();
    }

    public void loadFromString(String laserScan) {
        String vals[] = laserScan.split(",");
        rangeMax = Double.parseDouble(vals[0]);
        rangeMin = Double.parseDouble(vals[1]);
        angleMin = Double.parseDouble(vals[2]);
        angleMax = Double.parseDouble(vals[3]);
        angleIncrement = Double.parseDouble(vals[4]);
        pose = new DoubleOrientedPoint();
        pose.setX(Double.parseDouble(vals[5]));
        pose.setY(Double.parseDouble(vals[6]));
        pose.setTheta(Double.parseDouble(vals[7]));
        for (int i = 8; i < vals.length; i++) {
            ranges.add(Double.parseDouble(vals[i]));
        }
    }
}
