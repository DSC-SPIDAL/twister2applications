package edu.iu.dsc.tws.apps.slam.core.sensor;

import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

public class OdometryReading extends SensorReading {
    DoubleOrientedPoint m_pose;
    DoubleOrientedPoint m_speed;
    DoubleOrientedPoint m_acceleration;

    public OdometryReading(OdometrySensor m_sensor, double m_time) {
        super(m_time);
    }

    public DoubleOrientedPoint getPose() {
        return m_pose;
    }

    public DoubleOrientedPoint getSpeed() {
        return m_speed;
    }

    public DoubleOrientedPoint getAcceleration() {
        return m_acceleration;
    }

    public void setPose(DoubleOrientedPoint m_pose) {
        this.m_pose = m_pose;
    }

    public void setSpeed(DoubleOrientedPoint m_speed) {
        this.m_speed = m_speed;
    }

    public void setAcceleration(DoubleOrientedPoint m_acceleration) {
        this.m_acceleration = m_acceleration;
    }
}
