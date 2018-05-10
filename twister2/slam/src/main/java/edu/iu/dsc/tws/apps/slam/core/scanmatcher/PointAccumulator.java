package edu.iu.dsc.tws.apps.slam.core.scanmatcher;

import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;

/**
 * We put point accumulators to cells that are seen by the laser.
 * if n = 0 means, we have see the cell, but no obstacle
 * if n > 0 means a cell that has an obstacle
 */
public class PointAccumulator {
    public static final int SIGHT_INC = 1;

//    public DoublePoint acc = new DoublePoint(0.0, 0.0);

    public double accx = 0.0;
    public double accy = 0.0;

    public int n, visits;

    public PointAccumulator(PointAccumulator pointAccumulator) {
        this.n = pointAccumulator.n;
        this.visits = pointAccumulator.visits;
        this.accx = pointAccumulator.accx;
        this.accy = pointAccumulator.accy;
    }

    public PointAccumulator() {
    }

    public void update(boolean value, DoublePoint p) {
        if (value) {
            accx += p.x;
            accy += p.y;
            n++;
            visits += SIGHT_INC;
        } else {
            visits++;
        }
    }

    public void update(boolean value, double x, double y) {
        if (value) {
            accx += x;
            accy += y;
            n++;
            visits += SIGHT_INC;
        } else {
            visits++;
        }
    }

    public double entropy() {
        if (visits == 0)
            return -Math.log(.5);
        if (n == visits || n == 0)
            return 0;
        double x = (double) n * SIGHT_INC / (double) visits;
        return -(x * Math.log(x) + (1 - x) * Math.log(1 - x));
    }

    public DoublePoint mean() {
        return new DoublePoint(accx / n, accy / n);
    }

    void add(PointAccumulator p) {
        accx = accx + p.accx;
        accy = accy + p.accy;
        n +=p.n;
        visits += p.visits;
    }

    public double doubleValue() {
        return visits > 0 ? (double)n*SIGHT_INC/(double)visits:-1;
    }

    public int getN() {
        return n;
    }

    public int getVisits() {
        return visits;
    }

    public void setAcc(double accx, double accy) {
        this.accx = accx;
        this.accy = accy;
    }

    public void setN(int n) {
        this.n = n;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public double getAccx() {
        return accx;
    }

    public double getAccy() {
        return accy;
    }

    public void setAccx(double accx) {
        this.accx = accx;
    }

    public void setAccy(double accy) {
        this.accy = accy;
    }
}

