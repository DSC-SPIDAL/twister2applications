package edu.iu.dsc.tws.apps.slam.core.utils;

public class IntPoint {
    public int x;
    public int y;

    public IntPoint() {
    }

    public IntPoint(IntPoint p) {
        this.x = p.x;
        this.y = p.y;
    }

    public IntPoint(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public static IntPoint minus(IntPoint p1, IntPoint p2) {
        int x = p1.x - p2.x;
        int y = p1.y - p2.y;
        return new IntPoint(x, y);
    }

    public static IntPoint plus(IntPoint p1, IntPoint p2) {
        int x = p1.x + p2.x;
        int y = p1.y + p2.y;
        return new IntPoint(x, y);
    }

    public static IntPoint mul(IntPoint p1, IntPoint p2) {
        int x = p1.x * p2.x;
        int y = p1.y * p2.y;
        return new IntPoint(x, y);
    }

    public static double mulD(IntPoint p1, IntPoint p2) {
        int x = p1.x * p2.x;
        int y = p1.y * p2.y;
        return x + y;
    }

    public static double mulN(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x * p2.x;
        double y = p1.y * p2.y;
        return x + y;
    }

    public static IntPoint max(IntPoint p1, IntPoint p2){
        IntPoint p = p1;
        p.x = p.x > p2.x ? p.x : p2.x;
        p.y = p.y > p2.y ? p.y : p2.y;
        return p;
    }

    public static IntPoint min(IntPoint p1, IntPoint p2){
        IntPoint p = p1;
        p.x = p.x < p2.x ? p.x : p2.x;
        p.y = p.y < p2.y ? p.y : p2.y;
        return p;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntPoint intPoint = (IntPoint) o;

        if (x != intPoint.x) return false;
        if (y != intPoint.y) return false;

        return true;

//        boolean equal = this.x < intPoint.x || (this.x == intPoint.x) && this.y < intPoint.y;
//        return equal;
    }

    @Override
    public int hashCode() {
        int result = x;
        result = 31 * result + y;
        return result;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }
}
