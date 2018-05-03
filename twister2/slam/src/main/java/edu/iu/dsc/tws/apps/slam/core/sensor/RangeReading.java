package edu.iu.dsc.tws.apps.slam.core.sensor;

import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

public class RangeReading extends SensorReading {
    private DoubleOrientedPoint pose;

    public RangeReading() {
    }

    public RangeReading(double m_time) {
        super(m_time);
    }

    public RangeReading(int nBeams, Double []d, double time) {
        super(time);
        clear();
        for (int i = 0; i < nBeams; i++) {
            add(d[i]);
        }
    }

    public DoubleOrientedPoint getPose() {
        return pose;
    }

    public void setPose(DoubleOrientedPoint pose) {
        this.pose = pose;
    }
}
