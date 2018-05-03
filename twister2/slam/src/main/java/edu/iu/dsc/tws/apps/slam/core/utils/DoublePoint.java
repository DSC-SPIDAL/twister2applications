package edu.iu.dsc.tws.apps.slam.core.utils;

public class DoublePoint {
    public double x;
    public double y;

    public DoublePoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public DoublePoint(DoublePoint d) {
        x = d.x;
        y = d.y;
    }

    public DoublePoint() {
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public static DoublePoint minus(DoublePoint p1, DoublePoint p2) {
        double x = p1.x - p2.x;
        double y = p1.y - p2.y;
        return new DoublePoint(x, y);
    }

    public static DoublePoint plus(DoublePoint p1, DoublePoint p2) {
        double x = p1.x + p2.x;
        double y = p1.y + p2.y;
        return new DoublePoint(x, y);
    }

    public static DoublePoint mul(DoublePoint p1, DoublePoint p2) {
        double x = p1.x * p2.x;
        double y = p1.y * p2.y;
        return new DoublePoint(x, y);
    }

    public static double mulD(DoublePoint p1, DoublePoint p2) {
        double x = p1.x * p2.x;
        double y = p1.y * p2.y;
        return x + y;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }
}
