package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import java.util.ArrayList;

public class ParticleMapsList {
    private ArrayList<ParticleMaps> particleMapsArrayList = new ArrayList<ParticleMaps>();

    public ParticleMapsList() {
    }

    public ParticleMapsList(ArrayList<ParticleMaps> particleMapsArrayList) {
        this.particleMapsArrayList = particleMapsArrayList;
    }

    public ArrayList<ParticleMaps> getParticleMapsArrayList() {
        return particleMapsArrayList;
    }

    public void setParticleMapsArrayList(ArrayList<ParticleMaps> particleMapsArrayList) {
        this.particleMapsArrayList = particleMapsArrayList;
    }

    public void addParticleMap(ParticleMaps maps) {
        particleMapsArrayList.add(maps);
    }
}
