package edu.iu.dsc.tws.apps.slam.streaming.msgs;

public class PositionMessage {
    private double x;
    private double y;
    private double theta;

    public PositionMessage() {
    }

    public PositionMessage(double x, double y, double theta) {
        this.x = x;
        this.y = y;
        this.theta = theta;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public double getTheta() {
        return theta;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public void setTheta(double theta) {
        this.theta = theta;
    }
}
