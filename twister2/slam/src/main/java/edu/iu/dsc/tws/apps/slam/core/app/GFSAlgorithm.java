package edu.iu.dsc.tws.apps.slam.core.app;

import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.AbstractGridSlamProcessor;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.Particle;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GFSAlgorithm {
    private static Logger LOG = LoggerFactory.getLogger(GFSAlgorithm.class);

    public AbstractGridSlamProcessor gsp;
    double gspLaserAngleIncrement;
    double angleMin;
    double angleMax;
    int gspLaserBeamCount;

    int laserCount;
    int throttleScans;

    boolean gotFirstScan;
    long mapUpdateInterval = 20000;

    // Parameters used by GMapping
    double maxRange;
    double maxUrange;
    double minimumScore = 200;
    double sigma;
    int kernelSize;
    double lstep;
    double astep;
    int iterations;
    double lsigma;
    double ogain;
    int lskip;
    double srr;
    double srt;
    double str;
    double stt;
    double linearUpdate;
    double angularUpdate;
    double temporalUpdate;
    double resampleThreshold;
    int particles;
    double xmin;
    double ymin;
    double xmax;
    double ymax;
    double delta;
    double occThresh;
    double llsamplerange;
    double llsamplestep;
    double lasamplerange;
    double lasamplestep;
    int maxCorrections;

    private MapUpdater mapUpdater;

    public void init() {
        throttleScans = 1;

        // gmapping parameters
        maxUrange = 80;
        maxRange = 79.0;
        minimumScore = 0.0;
        sigma = 0.05;
        kernelSize = 1;
        lstep = 0.05;
        astep = 0.05;
        iterations = 5;
        lsigma = .075;
        ogain = 10.0;
        lskip = 0;
        srr = 0.1;
        srt = 0.2;
        str = 0.1;
        stt = 0.2;
        linearUpdate = 0.0;
        angularUpdate = 0.0;
        temporalUpdate = -1;
        resampleThreshold = 100;
        particles = 100;
        xmin = -70;
        ymin = -40.0;
        xmax = 10.0;
        ymax = 40.0;
        delta = 0.05;
        occThresh = 0.25;
        llsamplerange = 0.01;
        llsamplestep = 0.01;
        lasamplerange = 0.005;
        lasamplestep = 0.005;
        maxCorrections = 140;
    }

    public void setParticles(int particles) {
        this.particles = particles;
    }

    public boolean initMapper(LaserScan scan) {
        gspLaserBeamCount = scan.ranges.size();

        int orientationFactor = 1;

        angleMin = orientationFactor * scan.angleMin;
        angleMax = orientationFactor * scan.angleMax;
        gspLaserAngleIncrement = orientationFactor * scan.angleIncrement;
        LOG.debug("Laser angles top down in laser-frame: min: %.3f max: %.3f inc: %.3f", angleMin, angleMax, gspLaserAngleIncrement);

        DoubleOrientedPoint gmapPose = new DoubleOrientedPoint(0.0, 0.0, 0.0);

        // setting maxRange and maxUrange here so we can set a reasonable default
        maxRange = scan.rangeMax - 0.01;
        maxUrange = maxRange;

        double []laserAngles = Utils.getLaserAngles(gspLaserBeamCount, gspLaserAngleIncrement, angleMin);
        gsp.setLaserParams(gspLaserBeamCount, laserAngles, gmapPose);

        /// @todo Expose setting an initial pose
        DoubleOrientedPoint initialPose = new DoubleOrientedPoint(scan.getPose());
//        if (getOdomPose(initialPose, scan.timestamp) != null) {
//            LOG.warn("Unable to determine inital pose of laser! Starting point will be set to zero.");
//            initialPose = new DoubleOrientedPoint(0.0, 0.0, 0.0);
//        }

        gsp.setMatchingParameters(maxUrange, maxRange, sigma,
                kernelSize, lstep, astep, iterations,
                lsigma, ogain, lskip);

        gsp.setMotionModelParameters(srr, srt, str, stt);
        gsp.setUpdateDistances(linearUpdate, angularUpdate, resampleThreshold);
        gsp.setUpdatePeriod(temporalUpdate);
        gsp.getMatcher().setgenerateMap(false);
        gsp.init(particles, xmin, ymin, xmax, ymax, delta, initialPose);
        gsp.getMatcher().setLLSamplerange(llsamplerange);
        gsp.getMatcher().setLLSamplestep(llsamplestep);
        gsp.setMaxCorrections(maxCorrections);
        /// @todo Check these calls; in the gmapping gui, they use
        /// llsamplestep and llsamplerange intead of lasamplestep and
        /// lasamplerange.  It was probably a typo, but who knows.
        gsp.getMatcher().setLSSamplerange(lasamplerange);
        gsp.getMatcher().setLASamplestep(lasamplestep);
        gsp.setMinimumScore(minimumScore);

        // Call the sampling function once to set the seed.
        Stat.sampleGaussian(1, System.currentTimeMillis());

        LOG.info("Initialization complete");

        return true;
    }

    double totalTime = 0;
    double scanTime = 0;
    int count = 0;

    long lastMapUpdate;

    public void setLastMapUpdate(long lastMapUpdate) {
        this.lastMapUpdate = lastMapUpdate;
    }

    public void laserScan(LaserScan scan) {
        long t0 =  System.currentTimeMillis();
        laserCount++;
        if ((laserCount % throttleScans) != 0)
            return;



        // We can't initialize the mapper until we've got the first scan
        if (!gotFirstScan) {
            if (!initMapper(scan)) {
                return;
            }
            mapUpdater = new MapUpdater(maxRange, maxUrange, xmin, ymin, xmax, ymax, delta, occThresh);
            gotFirstScan = true;
        }

        DoubleOrientedPoint pose = scan.getPose();
        DoubleOrientedPoint odomPose = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        if (pose != null) {
            odomPose = pose;
        }

        if (addScan(scan)) {
            scanTime += System.currentTimeMillis() - t0;
            System.out.println("Add Scan Time: " + (System.currentTimeMillis() - t0) );
            LOG.debug("scan processed");

            DoubleOrientedPoint mpose = gsp.getParticles().get(gsp.getBestParticleIndex()).pose;
            LOG.debug("new best pose: %.3f %.3f %.3f", mpose.x, mpose.y, mpose.theta);
            LOG.debug("odom pose: %.3f %.3f %.3f", odomPose.x, odomPose.y, odomPose.theta);
            LOG.debug("correction: %.3f %.3f %.3f", mpose.x - odomPose.x, mpose.y - odomPose.y, mpose.theta - odomPose.theta);

            long t1 = System.currentTimeMillis();
            if ((scan.timestamp - lastMapUpdate) > mapUpdateInterval) {
//                updateMap(scan);
                LOG.debug("Updated the map");
                lastMapUpdate = System.currentTimeMillis();
//                lastMapUpdate = 0;
            }
            // System.out.println("Map compute Time: " + (System.currentTimeMillis() - t1) );
        }
        totalTime += System.currentTimeMillis() - t0;
        // System.out.println("Total Scan Time: " + (System.currentTimeMillis() - t0));
        count++;
        // System.out.println("Average scan time: " + scanTime / count);
        // System.out.println("Average total time: " + totalTime / count);
    }

    public boolean addScan(LaserScan scan) {
        DoubleOrientedPoint pose = scan.getPose();

        if (getOdomPose(pose, scan.timestamp) == null) {
            return false;
        }

        if (scan.ranges.size() != gspLaserBeamCount) {
            return false;
        }
        Double[] ranges = Utils.getRanges(scan, gspLaserAngleIncrement);
        RangeReading reading = new RangeReading(scan.ranges.size(),
                ranges,
                scan.timestamp);
        reading.setPose(pose);

        return gsp.processScan(reading, 0);
    }

    public GFSMap getMap() {
        return this.mapUpdater.getMap();
    }

    private DoubleOrientedPoint getOdomPose(DoubleOrientedPoint gmap_pose, long timestamp) {
        return gmap_pose;
    }

    public void updateMap(LaserScan scan) {
        double[] laserAngles = new double[scan.ranges.size()];
        laserAngles = Utils.getLaserAngles(scan.ranges.size(), scan.angleIncrement, angleMin);

        Particle best =
                gsp.getParticles().get(gsp.getBestParticleIndex());
        mapUpdater.updateMap(best, laserAngles, new DoubleOrientedPoint(0, 0, 0));
    }

    public static int MAP_IDX(int sx, int i, int j) {
        return ((sx) * (j) + (i));
    }
}
