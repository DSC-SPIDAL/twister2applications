package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import java.util.ArrayList;
import java.util.List;

public class ParticleValues {
    private List<ParticleValue> particleValues = new ArrayList<ParticleValue>();

    public ParticleValues(List<ParticleValue> particleValues) {
        this.particleValues = particleValues;
    }

    public ParticleValues() {
    }

    public List<ParticleValue> getParticleValues() {
        return particleValues;
    }

    public void setParticleValues(List<ParticleValue> particleValues) {
        this.particleValues = particleValues;
    }
}
