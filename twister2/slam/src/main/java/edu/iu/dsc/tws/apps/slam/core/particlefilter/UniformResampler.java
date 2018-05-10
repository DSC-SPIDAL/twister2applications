package edu.iu.dsc.tws.apps.slam.core.particlefilter;

import java.util.ArrayList;
import java.util.List;

public class UniformResampler {
    public static List<Integer> resampleIndexes(List<Double> particles, int nparticles) {
        double cweight = 0;

        //compute the cumulative weights
        int n = 0;
        for (Double it : particles) {
            cweight += it;
            n++;
        }

        if (nparticles > 0) {
            n = nparticles;
        }

        //compute the interval
        double interval = cweight / n;

        //compute the initial target weight
        double target = interval * (Math.random());
        //compute the resampled indexes
        cweight = 0;
        List<Integer> indexes = new ArrayList<Integer>(n);
        n = 0;
        int i = 0;
        for (Double it : particles) {
            cweight += it;
            while (cweight > target) {
                indexes.add(n++, i);
                target += interval;
            }
            i++;
        }
        return indexes;
    }
}
