package edu.iu.dsc.tws.apps.slam.core.particlefilter;

import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.Particle;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is not used
 */
public class ParticleFilter {
    public List<Integer> resampleP(List<Particle> particles) {
        double cweight = 0;

        //compute the cumulative weights
        int n = 0;
        for (Particle it : particles) {
            cweight += it.weight;
            n++;
        }

        //compute the interval
        double interval = cweight / n;

        //compute the initial target weight
        double target =
                //compute the resampled indexes

                cweight = 0;
        List<Integer> indexes = new ArrayList<Integer>(n);
        n = 0;
        int i = 0;
        for (Particle it : particles) {
            cweight += it.weight;
            while (cweight > target) {
                indexes.add(i);
                target += interval;
            }
            i++;
        }
        return indexes;
    }

    public List<Integer> resampleW(List<Double> weights) {
        double cweight = 0;

        //compute the cumulative weights
        int n = 0;
        for (Double it : weights) {
            cweight += it;
            n++;
        }

        //compute the interval
        double interval = cweight / n;

        //compute the initial target weight
        double target =
                //compute the resampled indexes

                cweight = 0;
        List<Integer> indexes = new ArrayList<Integer>(n);
        n = 0;
        int i = 0;
        for (Double it : weights) {
            cweight += it;
            while (cweight > target) {
                indexes.add(i);
                target += interval;
            }
            i++;
        }
        return indexes;
    }
}
