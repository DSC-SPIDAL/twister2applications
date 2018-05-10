package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import edu.iu.dsc.tws.apps.slam.core.scanmatcher.ScanMatcher;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractGridSlamProcessor {
    private Logger LOG = LoggerFactory.getLogger(AbstractGridSlamProcessor.class);

    public static final double distanceThresholdCheck = 20;

    protected List<Particle> particles = new ArrayList<Particle>();

    protected ScanMatcher matcher = new ScanMatcher();
    protected MotionModel motionModel = new MotionModel();

    protected int beams;
    protected double lastUpdateTime;
    protected double updatePeriod;
    protected double minimumScore;
    protected double resampleThreshold;

    protected int count, readingCount;
    protected DoubleOrientedPoint lastPartPose = new DoubleOrientedPoint(0.0, 0.0, 0.0);
    protected DoubleOrientedPoint odoPose = new DoubleOrientedPoint(0.0, 0.0, 0.0);
    protected DoubleOrientedPoint pose = new DoubleOrientedPoint(0.0, 0.0, 0.0);

    protected double linearDistance, angularDistance;

    protected double xmin;
    protected double ymin;
    protected double xmax;
    protected double ymax;

    protected double delta;
    protected double linearThresholdDistance;
    protected double angularThresholdDistance;
    protected double obsSigmaGain;

    protected List<Integer> activeParticles = new ArrayList<Integer>();

    // this lock must not be here. For now we will use it
    protected Lock lock = new ReentrantLock();

    protected Normalizer normalizer;

    protected ReSampler reSampler;

    public AbstractGridSlamProcessor() {
        updatePeriod = 100.0;
        obsSigmaGain = 1;
        resampleThreshold = 0.5;
        minimumScore = 0.;
    }

    public abstract void init(int size, double xmin, double ymin, double xmax, double ymax, double delta,
                              DoubleOrientedPoint initialPose, List<Integer> activeParticles);

    public abstract void init(int size, double xmin, double ymin, double xmax, double ymax, double delta,
                              DoubleOrientedPoint initialPose);

    public abstract boolean processScan(RangeReading reading, int adaptParticles);

    public abstract void scanMatch(double[] plainReading);

    public ScanMatcher getMatcher() {
        return matcher;
    }

    public List<Particle> getParticles() {
        return particles;
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

    public void setMinimumScore(double minimumScore) {
        this.minimumScore = minimumScore;
    }

    public void setUpdatePeriod(double period) {
        this.updatePeriod = period;
    }

    public void setMaxCorrections(int corrections) {
        this.matcher.setMaxCorrections(corrections);
    }

    public void setMatchingParameters(double urange, double range, double sigma, int kernsize, double lopt, double aopt,
                                      int iterations, double likelihoodSigma, double likelihoodGain, int likelihoodSkip) {
        obsSigmaGain = likelihoodGain;
        matcher.setMatchingParameters(urange, range, sigma, kernsize, lopt, aopt, iterations, likelihoodSigma, likelihoodSkip);
        LOG.info(" -maxUrange " + urange
                + " -maxUrange " + range
                + " -sigma     " + sigma
                + " -kernelSize " + kernsize
                + " -lstep " + lopt
                + " -lobsGain " + obsSigmaGain
                + " -astep " + aopt);
    }

    public void setMotionModelParameters
            (double srr, double srt, double str, double stt) {
        motionModel.srr = srr;
        motionModel.srt = srt;
        motionModel.str = str;
        motionModel.stt = stt;
        LOG.info(" -srr " + srr + " -srt " + srt + " -str " + str + " -stt " + stt);
    }

    public void setUpdateDistances(double linear, double angular, double resampleThreshold) {
        linearThresholdDistance = linear;
        angularThresholdDistance = angular;
        this.resampleThreshold = resampleThreshold;
        LOG.info(" -linearUpdate " + linear
                + " -angularUpdate " + angular
                + " -resampleThreshold " + this.resampleThreshold);
    }

    public void setLaserParams(int beams, double []angles, DoubleOrientedPoint laserPos) {
        this.beams = beams;
        matcher.setLaserParameters(beams, angles, laserPos);
    }

    public void clearActiveParticles() {
        lock.lock();
        try {
            activeParticles.clear();
        } finally {
            lock.unlock();
        }
    }

    public void addActiveParticle(int index) {
        lock.lock();
        try {
            if (!activeParticles.contains(index)) {
                activeParticles.add(index);
            }
        } finally {
            lock.unlock();
        }
    }

    public List<Integer> getActiveParticles() {
        return activeParticles;
    }

    protected double scanMatchParticle(double[] plainReading, double sumScore, Particle it) {
        DoubleOrientedPoint corrected = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        double score = 0, l;
        score = matcher.optimize(corrected, it.map, it.pose, plainReading);
        if (score > minimumScore) {
            LOG.debug("Score {}: Correcting the position from {} to {}", score, it.pose, corrected);
            it.pose = new DoubleOrientedPoint(corrected);
        } else {
            LOG.debug("Score {}:, Scan Matching Failed, using odometry. Likelihood: last pose P{}, odom pose {}", score, lastPartPose, odoPose);
        }

        ScanMatcher.LikeliHoodAndScore score1 = matcher.likelihoodAndScore(it.map, it.pose, plainReading);
        l = score1.l;

        sumScore += score;
        it.weight += l;
        it.weightSum += l;

        LOG.debug("Weigh of the particle: {}", it.weight);

        //set up the selective copy of the active area
        //by detaching the areas that will be updated
        matcher.invalidateActiveArea();
        matcher.computeActiveArea(it.map, it.pose, plainReading);
        return sumScore;
    }
}
