package edu.iu.dsc.tws.apps.slam.core.sensor;

public class OdometrySensor extends Sensor {
    boolean m_ideal =  false;

    public OdometrySensor(String name, boolean ideal) {
        super(name);
        m_ideal = ideal;
    }

    public boolean isIdeal() {
        return m_ideal;
    }
}
