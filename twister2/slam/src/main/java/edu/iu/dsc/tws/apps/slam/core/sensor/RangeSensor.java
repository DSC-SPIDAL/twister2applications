package edu.iu.dsc.tws.apps.slam.core.sensor;

import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;

import java.util.ArrayList;
import java.util.List;

public class RangeSensor extends Sensor {
    DoubleOrientedPoint m_pose;
    List<Beam> m_beams = new ArrayList<Beam>();
    boolean newFormat;

    public RangeSensor() {
    }

    public RangeSensor(String name, int beams, double res, DoubleOrientedPoint position, double span, double maxrange) {
        super(name);
        this.m_pose = position;
        double angle = -.5 * res * beams;
       // double angle = 0;
        for (int i = 0; i < beams; i++, angle += res) {
            Beam beam = new Beam();
            beam.span = span;
            beam.pose.x = 0.0;
            beam.pose.y = 0.0;
            beam.pose.theta = angle;
            beam.maxRange = maxrange;
            m_beams.add(beam);
        }
        newFormat = false;
        updateBeamsLookup();
    }

    void updateBeamsLookup() {
        for (int i = 0; i < m_beams.size(); i++) {
            Beam beam = m_beams.get(i);
            beam.s = Math.sin(m_beams.get(i).pose.theta);
            beam.c = Math.cos(m_beams.get(i).pose.theta);
        }
    }

    public class Beam {
        public DoubleOrientedPoint pose = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        public double span;
        public double maxRange;
        public double s, c;
    }

    public List<Beam> beams() {
        return m_beams;
    }

    public DoubleOrientedPoint getPose() {
        return m_pose;
    }

    public DoubleOrientedPoint getM_pose() {
        return m_pose;
    }

    public List<Beam> getM_beams() {
        return m_beams;
    }

    public boolean isNewFormat() {
        return newFormat;
    }

    public void setM_pose(DoubleOrientedPoint m_pose) {
        this.m_pose = m_pose;
    }

    public void setM_beams(List<Beam> m_beams) {
        this.m_beams = m_beams;
    }

    public void setNewFormat(boolean newFormat) {
        this.newFormat = newFormat;
    }
}
