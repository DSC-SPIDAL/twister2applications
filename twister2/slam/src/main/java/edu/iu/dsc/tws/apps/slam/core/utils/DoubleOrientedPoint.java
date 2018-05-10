package edu.iu.dsc.tws.apps.slam.core.utils;

public class DoubleOrientedPoint {
    public double theta;

    public double x;

    public double y;

    public DoubleOrientedPoint() {
    }

    public DoubleOrientedPoint(double x, double y) {
        this(x, y, 0.0);
    }

    public DoubleOrientedPoint(double x, double y, double theta) {
        this.x = x;
        this.y = y;
        this.theta = theta;
    }

    public DoubleOrientedPoint(DoubleOrientedPoint p) {
        this.x = p.x;
        this.y = p.y;
        this.theta = p.theta;
    }

    public static DoubleOrientedPoint minus(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x - p2.x;
        double y = p1.y - p2.y;
        double theta = Utils.theta(p1.theta - p2.theta);
        return new DoubleOrientedPoint(x, y, theta);
    }

    public static DoubleOrientedPoint plus(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x + p2.x;
        double y = p1.y + p2.y;
        double theta = Utils.theta(p1.theta + p2.theta);
        return new DoubleOrientedPoint(x, y, theta);
    }

    public static DoubleOrientedPoint mul(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x * p2.x;
        double y = p1.y * p2.y;
//        double theta = p1.theta + p2.theta;
        return new DoubleOrientedPoint(x, y, p1.theta);
    }

    public static double mulN(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x * p2.x;
        double y = p1.y * p2.y;
        return x + y;
    }

    public static DoubleOrientedPoint mulN(DoubleOrientedPoint p1, double p2) {
        double x = p1.x * p2;
        double y = p1.y * p2;
        return new DoubleOrientedPoint(x, y, Utils.theta(p1.theta * p2));
    }

    // TODO check
    public static double mulD(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x * p2.x;
        double y = p1.y * p2.y;
        return x + y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DoubleOrientedPoint that = (DoubleOrientedPoint) o;

        if (Double.compare(that.theta, theta) != 0) return false;
        if (Double.compare(that.x, x) != 0) return false;
        if (Double.compare(that.y, y) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(theta);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(y);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public double getTheta() {
        return theta;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setTheta(double theta) {
        this.theta = theta;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "DoubleOrientedPoint{" +
                "theta=" + theta +
                ", x=" + x +
                ", y=" + y +
                '}';
    }
}
