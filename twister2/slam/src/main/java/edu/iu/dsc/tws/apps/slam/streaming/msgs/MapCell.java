package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;

public class MapCell {
    private int x;
    private int y;

    private double accx = 0.0;
    private double accy = 0.0;

    private int n, visits;

    public MapCell() {
    }

    public MapCell(int x, int y, double accx, double accy, int n, int visits) {
        this.x = x;
        this.y = y;
        this.accx = accx;
        this.accy = accy;
        this.n = n;
        this.visits = visits;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public double getAccx() {
        return accx;
    }

    public double getAccy() {
        return accy;
    }

    public int getN() {
        return n;
    }

    public int getVisits() {
        return visits;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

    public void setAccx(double accx) {
        this.accx = accx;
    }

    public void setAccy(double accy) {
        this.accy = accy;
    }

    public void setN(int n) {
        this.n = n;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }
}
