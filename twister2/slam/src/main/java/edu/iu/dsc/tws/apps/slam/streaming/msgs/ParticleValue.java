package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

import java.util.List;

/**
 * Particle without the map. This will be emited by the ScanMatchBolt,
 * The resampling bolt will use these values to choose the best particles.
 */
public class ParticleValue {
    /** the task that sent the particle */
    private int taskId;
    /** The index of the particle */
    private int index;
    /** The pose of the robot */
    private DoubleOrientedPoint pose;

    /** The pose of the robot at the previous time frame (used for computing thr odometry displacements) */
    private DoubleOrientedPoint previousPose;

    /** The weight of the particle */
    private double weight;

    /** The cumulative weight of the particle */
    private double weightSum;

    private double gweight;

    /** The index of the previous particle in the trajectory tree */
    private int previousIndex;

    /** Node tree as a list */
    private List<TNodeValue> nodes;

    private int totalTasks;

    private boolean best = false;

    public ParticleValue() {
    }

    public ParticleValue(int taskId, int index, int totalTasks, DoubleOrientedPoint pose,
                         DoubleOrientedPoint previousPose, double weight,
                         double weightSum, double gweight, int previousIndex,
                         List<TNodeValue> node) {
        this.taskId = taskId;
        this.index = index;
        this.pose = pose;
        this.previousPose = previousPose;
        this.weight = weight;
        this.weightSum = weightSum;
        this.gweight = gweight;
        this.previousIndex = previousIndex;
        this.nodes = node;
        this.totalTasks = totalTasks;
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTotalTasks(int totalTasks) {
        this.totalTasks = totalTasks;
    }

    public int getIndex() {
        return index;
    }

    public DoubleOrientedPoint getPose() {
        return pose;
    }

    public DoubleOrientedPoint getPreviousPose() {
        return previousPose;
    }

    public double getWeight() {
        return weight;
    }

    public double getWeightSum() {
        return weightSum;
    }

    public double getGweight() {
        return gweight;
    }

    public int getPreviousIndex() {
        return previousIndex;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setPose(DoubleOrientedPoint pose) {
        this.pose = pose;
    }

    public void setPreviousPose(DoubleOrientedPoint previousPose) {
        this.previousPose = previousPose;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public void setWeightSum(double weightSum) {
        this.weightSum = weightSum;
    }

    public void setGweight(double gweight) {
        this.gweight = gweight;
    }

    public void setPreviousIndex(int previousIndex) {
        this.previousIndex = previousIndex;
    }

//    public void setNode(TNode node) {
//        this.node = node;
//    }

    public List<TNodeValue> getNodes() {
        return nodes;
    }

    public void setNodes(List<TNodeValue> nodes) {
        this.nodes = nodes;
    }

    public boolean isBest() {
        return best;
    }

    public void setBest(boolean best) {
        this.best = best;
    }
}
