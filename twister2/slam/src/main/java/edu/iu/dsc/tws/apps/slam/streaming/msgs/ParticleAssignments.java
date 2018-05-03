package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import java.util.ArrayList;
import java.util.List;

public class ParticleAssignments {
    private List<ParticleAssignment> assignments = new ArrayList<ParticleAssignment>();

    /** If the resampler doesn't re sample there will be no assignments */
    private boolean reSampled = false;

    private int bestParticle;

    private Trace trace;

    public ParticleAssignments() {
    }

    public Trace getTrace() {
        return trace;
    }

    public void setTrace(Trace trace) {
        this.trace = trace;
    }

    public int getBestParticle() {
        return bestParticle;
    }

    public void setBestParticle(int bestParticle) {
        this.bestParticle = bestParticle;
    }

    public List<ParticleAssignment> getAssignments() {
        return assignments;
    }

    public void setAssignments(List<ParticleAssignment> assignments) {
        this.assignments = assignments;
    }

    public void addAssignment(ParticleAssignment assignment) {
        assignments.add(assignment);
    }

    public boolean isReSampled() {
        return reSampled;
    }

    public void setReSampled(boolean reSampled) {
        this.reSampled = reSampled;
    }
}
