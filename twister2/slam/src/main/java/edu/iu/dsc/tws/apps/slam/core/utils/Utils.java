package edu.iu.dsc.tws.apps.slam.core.utils;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static Double[] getRanges(LaserScan scan, double gspLaserAngleIncrement) {
        // GMapping wants an array of doubles...
        Double[] ranges = new Double[scan.getRanges().size()];
        // If the angle increment is negative, we have to invert the order of the readings.
        if (gspLaserAngleIncrement < 0) {
            LOG.debug("Inverting scan");
            int num_ranges = scan.getRanges().size();
            for (int i = 0; i < num_ranges; i++) {
                // Must filter out short readings, because the mapper won't
                if (scan.getRanges().get(i) < scan.getRangeMin()) {
                    ranges[i] = scan.getRangeMax();
                } else {
                    ranges[i] = scan.getRanges().get(num_ranges - i - 1);
                }
            }
        } else {
            for (int i = 0; i < scan.getRanges().size(); i++) {
                // Must filter out short readings, because the mapper won't
                if (scan.getRanges().get(i) < scan.getRangeMin()) {
                    ranges[i] = scan.getRangeMax();
                } else {
                    ranges[i] = scan.getRanges().get(i);
                }
            }
        }
        return ranges;
    }

    public static double[] getLaserAngles(int beams, double angleIncrement, double angleMin) {
//        double angle = -.5 * angleIncrement * beams;
        double angle = angleMin;
        double []angles = new double[beams];
//        double angle = 0;
        for (int i = 0; i < beams; i++, angle += angleIncrement) {
            angles[i] = angle;
        }
        return angles;
    }

    public static double theta(double t) {
//        return Math.atan2(Math.sin(t), Math.cos(t));
        return t;
    }
}
