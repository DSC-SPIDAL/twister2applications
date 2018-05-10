package edu.iu.dsc.tws.apps.slam.core.sensor;

import java.util.ArrayList;

public class SensorReading extends ArrayList<Double> {
    double time;

    public SensorReading() {
    }

    public SensorReading(double mTime) {
        this.time = mTime;
    }

    public double getTime() {
        return time;
    }

    public void setTime(double time) {
        this.time = time;
    }
}
