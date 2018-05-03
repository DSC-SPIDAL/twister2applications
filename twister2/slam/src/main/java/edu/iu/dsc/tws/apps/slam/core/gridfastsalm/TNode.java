package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

public class TNode {
    /**The pose of the robot*/
    public DoubleOrientedPoint pose;

    /**The weight of the particle*/
    public double weight;

    /**The sum of all the particle weights in the previous part of the trajectory*/
    public double accWeight;

    public double gweight;

    /**The parent*/
    public TNode parent;

    /**The range reading to which this node is associated*/
    public RangeReading reading;

    /**The number of childs*/
    public int childs;

    /**counter in visiting the node (internally used)*/
    public int visitCounter;

    /**visit flag (internally used)*/
    public boolean flag;

    public TNode(DoubleOrientedPoint p, double w, TNode n, int c) {
        pose = p;
        weight = w;
        childs = c;
        if (n != null) {
            parent = new TNode(n);
        } else {
            parent = null;
        }
        reading = null;
        gweight = 0;
        if (n != null) {
            n.childs++;
        }
        flag = false;
        accWeight = 0;
    }

    public TNode(TNode t) {
        this.pose = t.pose;
        this.weight = t.weight;
        this.childs = t.childs;
        this.parent = t.parent;
        this.reading = t.reading;
        this.gweight = t.getGweight();
        this.flag = t.flag;
        this.accWeight = t.accWeight;
    }

    public TNode() {
    }

    public TNode(DoubleOrientedPoint pose, double weight,
                 double accWeight, double gweight,
                 TNode parent, RangeReading reading,
                 int childs, int visitCounter, boolean flag) {
        this.pose = pose;
        this.weight = weight;
        this.accWeight = accWeight;
        this.gweight = gweight;
        this.parent = parent;
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

    public TNode getParent() {
        return parent;
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

    public void setParent(TNode parent) {
        this.parent = parent;
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
