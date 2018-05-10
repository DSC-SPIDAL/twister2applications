package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GridSlamProcessor extends SharedMemoryGridSlamProcessor {
    private static Logger LOG = LoggerFactory.getLogger(GridSlamProcessor.class);

    /**
     * Just scan match every single particle.
     * If the scan matching fails, the particle gets a default likelihood.
     */
    public void scanMatch(double[] plainReading) {
        // sample a new pose from each scan in the reference
        double sumScore = 0;
        for (Particle it : particles) {
            sumScore += scanMatchParticle(plainReading, 0, it);
        }
        LOG.info("Average Scan Matching Score = " + sumScore / particles.size());
    }
}
