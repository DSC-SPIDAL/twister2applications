package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.*;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DistributedReSampler {
    private static Logger LOG = LoggerFactory.getLogger(DistributedReSampler.class);

    protected List<Particle> particles = new ArrayList<Particle>();

    protected double resampleThreshold;

    protected int count;
    protected double obsSigmaGain;

    protected ReSampler reSampler;

    protected Normalizer normalizer;

    protected int noOfParticles;

    public DistributedReSampler(double obsSigmaGain, double resampleThreshold, int noOfParticles) {
        this.obsSigmaGain = obsSigmaGain;
        this.resampleThreshold = resampleThreshold;
        this.normalizer = new Normalizer(obsSigmaGain);
        this.reSampler = new ReSampler(resampleThreshold, false);
        this.noOfParticles = noOfParticles;
    }

    public List<Particle> getParticles() {
        return particles;
    }

    public int getNoOfParticles() {
        return noOfParticles;
    }

    public int getBestParticleIndex() {
        int bi = 0;
        double bw = -Double.MAX_VALUE;
        for (int i = 0; i < particles.size(); i++)
            if (bw < particles.get(i).weightSum) {
                bw = particles.get(i).weightSum;
                bi = i;
            }
        return bi;
    }

    public ReSampler.ReSampleResult processScan(RangeReading reading, int adaptParticles) {
        // here there is no need to check weather we need to perform the calculation.
        // If we are at this point we need to do the calculation
        //this is for converting the reading in a scan-matcher feedable form
        double[] plainReading = new double[reading.size()];
        for (int i = 0; i < reading.size(); i++) {
            plainReading[i] = reading.get(i);
        }

        RangeReading readingCopy =
                new RangeReading(reading.size(), reading.toArray(new Double[reading.size()]),
                        reading.getTime());

        ReSampler.ReSampleResult hasRsSampled;
        if (count > 0) {
            Normalizer.NormalizeResult result = normalizer.updateTreeWeights(false, particles);
            hasRsSampled = reSampler.resample(particles, result.getNeff(),
                plainReading, adaptParticles, readingCopy, result.getWeights());
        } else {
            hasRsSampled = new ReSampler.ReSampleResult(false, null);
        }
        count++;
        normalizer.updateTreeWeights(false, particles);

        //update the past pose for the next iteration
        //keep ready for the next step
        for (Particle it : particles) {
            it.previousPose = it.pose;
        }

        return hasRsSampled;
    }
}
