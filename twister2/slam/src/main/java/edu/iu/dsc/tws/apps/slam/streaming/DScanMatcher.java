package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.grid.IGMap;
import edu.iu.dsc.tws.apps.slam.core.grid.MapFactory;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.*;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DScanMatcher extends AbstractGridSlamProcessor {
    private static Logger LOG = LoggerFactory.getLogger(DScanMatcher.class);

    private int noOfParticles;

    private List<RangeReading> readings = new ArrayList<RangeReading>();

    public List<RangeReading> getReadings() {
        return readings;
    }

    public void init(int size, double xmin, double ymin, double xmax, double ymax, double delta, DoubleOrientedPoint initialPose, List<Integer> activeParticles) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
        this.delta = delta;

        LOG.info(" -xmin " + this.xmin + " -xmax " + this.xmax + " -ymin " + this.ymin
                + " -ymax " + this.ymax + " -delta " + this.delta + " -particles " + size);

        particles.clear();

        for (int i = 0; i < size; i++) {
            IGMap lmap;
            if (activeParticles.contains(i)) {
                lmap = MapFactory.create(new DoublePoint((xmin + xmax) * .5, (ymin + ymax) * .5), xmax - xmin, ymax - ymin, delta);
            } else {
                lmap = MapFactory.create(new DoublePoint((0) * .5, (0) * .5), 2, 2, delta);
            }
            Particle p = new Particle(lmap);

            p.pose = new DoubleOrientedPoint(initialPose);
            p.previousPose = initialPose;
            p.setWeight(0);
            p.previousIndex = 0;
            particles.add(p);
            // we use the root directly
            p.node = new TNode(initialPose, 0, null, 0);
        }
        noOfParticles = size;
        count = 0;
        readingCount = 0;
        linearDistance = angularDistance = 0;

        normalizer = new Normalizer(obsSigmaGain);
        reSampler = new ReSampler(resampleThreshold);
    }

    public void initParticles(DoubleOrientedPoint initialPose) {
        particles.clear();

        for (int i = 0; i < noOfParticles; i++) {
            IGMap lmap;
            if (activeParticles.contains(i)) {
                lmap = MapFactory.create(new DoublePoint((xmin + xmax) * .5, (ymin + ymax) * .5), xmax - xmin, ymax - ymin, delta);
            } else {
                lmap = MapFactory.create(new DoublePoint((0) * .5, (0) * .5), 2, 2, delta);
            }
            Particle p = new Particle(lmap);

            p.pose = new DoubleOrientedPoint(initialPose);
            p.previousPose = initialPose;
            p.setWeight(0);
            p.previousIndex = 0;
            particles.add(p);
            // we use the root directly
            p.node = new TNode(initialPose, 0, null, 0);
        }
    }

    @Override
    public void init(int size, double xmin, double ymin, double xmax, double ymax, double delta, DoubleOrientedPoint initialPose) {
        throw new IllegalArgumentException("This method is not implemented");
    }


    private RangeReading reading;

    @Override
    public boolean processScan(RangeReading reading, int adaptParticles) {
        long t0 = System.currentTimeMillis();
        DoubleOrientedPoint relPose = reading.getPose();
        if (count == 0) {
            lastPartPose = odoPose = relPose;
        }

//        LOG.info("Got laser pose: {}", reading.getPose());

        for (Particle p : particles) {
            p.pose = motionModel.drawFromMotion(p.pose, relPose, odoPose);
            LOG.debug("After motion: {}, {}", relPose, p.pose);
        }
        this.reading = reading;
        DoubleOrientedPoint move = DoubleOrientedPoint.minus(relPose, odoPose);
        move.theta = Math.atan2(Math.sin(move.theta), Math.cos(move.theta));
        linearDistance += Math.sqrt(DoubleOrientedPoint.mulN(move, move));
        angularDistance += Math.abs(move.theta);

        // if the robot jumps throw a warning
        if (linearDistance > distanceThresholdCheck) {
            LOG.error("Robot jumped too much ************************");
            odoPose = relPose;
        } else if (angularDistance > Math.PI / 2) {
            LOG.error("Robot jumped too much ************************");
            odoPose = relPose;
        }

        odoPose = relPose;
        boolean processed = false;

        // process a scan only if the robot has traveled a given distance or a certain amount of time has elapsed
        if (count == 0
                || linearDistance >= linearThresholdDistance
                || angularDistance >= angularThresholdDistance
                || (updatePeriod >= 0.0 && (reading.getTime() - lastUpdateTime) > updatePeriod)) {
            lastUpdateTime = reading.getTime();

            //this is for converting the reading in a scan-matcher feedable form
            if (reading.size() != beams) {
                throw new IllegalStateException("reading should contain " + beams + " beams");
            }
            double[] plainReading = new double[beams];
            for (int i = 0; i < beams; i++) {
                plainReading[i] = reading.get(i);
            }

            if (count > 0) {
                scanMatch(plainReading);

                // updateTreeWeights(false);
                // resample(plainReading, adaptParticles, readingCopy);
            } else {
                for (int index : activeParticles) {
                    Particle it = particles.get(index);
                    matcher.invalidateActiveArea();
                    matcher.computeActiveArea(it.map, it.pose, plainReading);
                    matcher.registerScan(it.map, it.pose, plainReading);

                    // not needed anymore, particles refer to the root in the beginning!
                    TNode node = new TNode(it.pose, 0., it.node, 0);
                    node.reading = reading;
                    it.node = node;
                }
            }

            // we add the reading here
            readings.add(reading);

            lastPartPose = odoPose; //update the past pose for the next iteration
            linearDistance = 0;
            angularDistance = 0;
            count++;
            processed = true;

            //keep ready for the next step
            for (Particle it : particles) {
                it.previousPose = it.pose;
            }

        }
        readingCount++;
        LOG.debug("Time in process scan: {} *****************************", (System.currentTimeMillis() - t0));
        return processed;
    }

    @Override
    public void scanMatch(double[] plainReading) {
        // sample a new pose from each scan in the reference
        double sumScore = 0;
        lock.lock();
        try {
            for (int index : activeParticles) {
                Particle it = particles.get(index);
                sumScore += scanMatchParticle(plainReading, sumScore, it);
            }
        }finally {
            lock.unlock();
        }
        LOG.debug("Average Scan Matching Score =" + sumScore / particles.size());
    }

    public void processAfterReSampling(double []plainReading) {
        lock.lock();
        try {
            for (int i : activeParticles) {
                Particle it = particles.get(i);

                TNode node;
                TNode oldNode = it.getNode();
                node = new TNode(it.pose, 0, oldNode, 0);
                node.reading = this.reading;

                it.node = node;

                matcher.invalidateActiveArea();
                matcher.registerScan(it.map, it.pose, plainReading);
                // particles.add(it);
            }
        } finally {
            lock.unlock();
        }
    }

    public void postProcessingWithoutReSampling(double []plainReading, RangeReading reading) {
        List<TNode> oldGeneration = new ArrayList<TNode>();
        for (int i : activeParticles) {
            Particle m_particle = particles.get(i);
            oldGeneration.add(m_particle.node);
        }
        int index = 0;
        LOG.debug("Post processing without resampling.....");
        Iterator<TNode> node_it = oldGeneration.iterator();
        lock.lock();
        try {
            for (int i : activeParticles) {
                Particle it = particles.get(i);
                //create a new node in the particle tree and add it to the old tree
                TNode node;
                node = new TNode(it.pose, 0.0, node_it.next(), 0);

                node.reading = reading;
                it.node = node;

                matcher.invalidateActiveArea();
                matcher.registerScan(it.map, it.pose, plainReading);
                it.previousIndex = index;
                index++;
            }
        } finally {
            lock.unlock();
        }
    }
}
