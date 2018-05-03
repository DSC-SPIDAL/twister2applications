package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;


public class TNodeValue {
    /**The pose of the robot*/
    private DoubleOrientedPoint pose;

    /**The weight of the particle*/
    private double weight;

    /**The sum of all the particle weights in the previous part of the trajectory*/
    private double accWeight;

    private double gweight;

    /**The range reading to which this node is associated*/
    private RangeReading reading;

    /**The number of childs*/
    private int childs;

    /**counter in visiting the node (internally used)*/
    private int visitCounter;

    /**visit flag (internally used)*/
    private boolean flag;

    public TNodeValue() {
    }

    public TNodeValue(DoubleOrientedPoint pose, double weight, double accWeight,
                      double gweight, RangeReading reading,
                      int childs, int visitCounter, boolean flag) {
        this.pose = pose;
        this.weight = weight;
        this.accWeight = accWeight;
        this.gweight = gweight;
        this.reading = reading;
        this.childs = childs;
        this.visitCounter = visitCounter;
        this.flag = flag;
    }

    public DoubleOrientedPoint getPose() {
        return pose;
    }

    public double getWeight() {
        return weight;
    }

    public double getAccWeight() {
        return accWeight;
    }

    public double getGweight() {
        return gweight;
    }

    public RangeReading getReading() {
        return reading;
    }

    public int getChilds() {
        return childs;
    }

    public int getVisitCounter() {
        return visitCounter;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setPose(DoubleOrientedPoint pose) {
        this.pose = pose;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public void setAccWeight(double accWeight) {
        this.accWeight = accWeight;
    }

    public void setGweight(double gweight) {
        this.gweight = gweight;
    }

    public void setReading(RangeReading reading) {
        this.reading = reading;
    }

    public void setChilds(int childs) {
        this.childs = childs;
    }

    public void setVisitCounter(int visitCounter) {
        this.visitCounter = visitCounter;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
