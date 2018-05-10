package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import edu.iu.dsc.tws.apps.slam.core.grid.IGMap;
import edu.iu.dsc.tws.apps.slam.core.grid.MapFactory;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public abstract class SharedMemoryGridSlamProcessor extends AbstractGridSlamProcessor {
    private static Logger LOG = LoggerFactory.getLogger(SharedMemoryGridSlamProcessor.class);

    public void init(int size, double xmin, double ymin, double xmax, double ymax, double delta, DoubleOrientedPoint initialPose,
                     List<Integer> activeParticles) {
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

        count = 0;
        readingCount = 0;
        linearDistance = angularDistance = 0;

        normalizer = new Normalizer(obsSigmaGain);
        reSampler = new ReSampler(resampleThreshold);
    }

    public void init(int size, double xmin, double ymin, double xmax, double ymax, double delta, DoubleOrientedPoint initialPose) {
        List<Integer> activeParticles = new ArrayList<Integer>();
        for (int i = 0; i < size; i++) {
            activeParticles.add(i);
        }
        init(size, xmin, ymin, xmax, ymax, delta, initialPose, activeParticles);
    }

    public boolean processScan(RangeReading reading, int adaptParticles) {
        DoubleOrientedPoint relPose = reading.getPose();
        if (count == 0) {
            lastPartPose = odoPose = relPose;
        }

        for (Particle p : particles) {
            p.pose = motionModel.drawFromMotion(p.pose, relPose, odoPose);
        }

        LOG.info("ODOM " + odoPose.x + " " + odoPose.y + " " + odoPose.theta + " " + reading.getTime());
//        LOG.info("ODO_UPDATE " + particles.size() + " ");
//        LOG.info("ODO_UPDATE Time {}", reading.getTime());

        DoubleOrientedPoint move = DoubleOrientedPoint.minus(relPose, odoPose);
        move.theta = Math.atan2(Math.sin(move.theta), Math.cos(move.theta));
        linearDistance += Math.sqrt(DoubleOrientedPoint.mulN(move, move));
        angularDistance += Math.abs(move.theta);

        // if the robot jumps throw a warning
        if (linearDistance > distanceThresholdCheck) {
            LOG.error("Robot jumped too much ************************");
            odoPose = relPose;
            return true;
        } else if (angularDistance > Math.PI / 2) {
            LOG.error("Robot jumped too much ************************");
            odoPose = relPose;
            return true;
        }

        odoPose = relPose;
        boolean processed = false;

        // process a scan only if the robot has traveled a given distance or a certain amount of time has elapsed
        if (count == 0
                || linearDistance >= linearThresholdDistance
                || angularDistance >= angularThresholdDistance
                || (updatePeriod >= 0.0 && (reading.getTime() - lastUpdateTime) > updatePeriod)) {
            lastUpdateTime = reading.getTime();

            LOG.info("FRAME " + readingCount + " " + linearDistance + " " + angularDistance);

            LOG.info("update frame " + readingCount + "update ld=" + linearDistance + " ad=" + angularDistance);
            LOG.info("Laser Pose= " + reading.getPose().x + " " + reading.getPose().y + " " + reading.getPose().theta);

            //this is for converting the reading in a scan-matcher feedable form
            double[] plainReading = new double[beams];
            for (int i = 0; i < beams; i++) {
                plainReading[i] = reading.get(i);
            }

            LOG.info("count " + count);
            if (count > 0) {
                scanMatch(plainReading);
                LOG.debug("LASER_READING " + reading.size() + " ");
                for (Double b : reading) {
                    LOG.debug(b + " ");
                }
                DoubleOrientedPoint p = reading.getPose();

                Normalizer.NormalizeResult result = normalizer.updateTreeWeights(false, particles);
                LOG.info("neff = " + result.getNeff());
                ReSampler.ReSampleResult reSampled = reSampler.resample(particles, result.getNeff(), plainReading, adaptParticles, reading, result.getWeights());
                if (reSampled.isReSampled()) {
                    for (Particle it : particles) {
                        matcher.invalidateActiveArea();
                        matcher.registerScan(it.map, it.pose, plainReading);
                    }
                } else {
                    int index = 0;
                    List<TNode> oldGeneration = new ArrayList<TNode>();
                    for (Particle m_particle : particles) {
                        oldGeneration.add(m_particle.node);
                    }
                    Iterator<TNode> nodeIt = oldGeneration.iterator();
                    for (Particle it : particles) {
                        //create a new node in the particle tree and add it to the old tree
                        TNode node;
                        node = new TNode(it.pose, 0.0, nodeIt.next(), 0);
                        node.reading = reading;
                        it.node = node;

                        matcher.invalidateActiveArea();
                        matcher.registerScan(it.map, it.pose, plainReading);
                        it.previousIndex = index;
                        index++;
                    }
                }
            } else {
                LOG.info("Registering First Scan");
                for (Particle it : particles) {
                    matcher.invalidateActiveArea();
                    matcher.computeActiveArea(it.map, it.pose, plainReading);
                    matcher.registerScan(it.map, it.pose, plainReading);

                    // not needed anymore, particles refer to the root in the beginning!
                    TNode node = new TNode(it.pose, 0., it.node, 0);
                    node.reading = reading;
                    it.node = node;
                }
            }
            normalizer.updateTreeWeights(false, particles);

            lastPartPose = odoPose; //update the past pose for the next iteration
            linearDistance = 0;
            angularDistance = 0;
            count++;
            processed = true;

            //keep ready for the next step
            for (Particle it : particles) {
                it.previousPose = it.pose;
            }
        } else {
            LOG.info("Not calculating: Linear update {} Angular Update {}", linearDistance, angularDistance);
        }
        readingCount++;
        return processed;
    }
}
