package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Normalizer {
    private static Logger LOG = LoggerFactory.getLogger(Normalizer.class);

    protected List<Double> weights = new ArrayList<Double>();

    protected double obsSigmaGain;

    public Normalizer(double obsSigmaGain) {
        this.obsSigmaGain = obsSigmaGain;
    }

    public void resetTree(List<Particle> particles) {
        // don't calls this function directly, use updateTreeWeights(..) !
        for (Particle it : particles) {
            TNode n = it.node;
            while (n != null) {
                n.accWeight = 0;
                n.visitCounter = 0;
                n = n.parent;
            }
        }
    }

    public double propagateWeight(TNode n, double weight, List<Particle> particles) {
        if (n == null) {
            return weight;
        }
        double w = 0;
        n.visitCounter++;
        n.accWeight += weight;
        if (n.visitCounter == n.childs) {
            w = propagateWeight(n.parent, n.accWeight, particles);
        }
        assert (n.visitCounter <= n.childs);
        return w;
    }

    public double propagateWeights(List<Particle> particles) {
        // don't calls this function directly, use updateTreeWeights(..) !
        // all nodes must be resetted to zero and weights normalized
        // the accumulated weight of the root
        double lastNodeWeight = 0;
        // sum of the weights in the leafs
        double aw = 0;

        int count = 0;
        for (Particle particle : particles) {
            double weight = weights.get(count++);
            aw += weight;
            TNode n = particle.node;
            n.accWeight = weight;
            lastNodeWeight += propagateWeight(n.parent, n.accWeight, particles);
        }

        if (Math.abs(aw - 1.0) > 0.0001 || Math.abs(lastNodeWeight - 1.0) > 0.0001) {
            //LOG.error("ERROR: root->accWeight=" + lastNodeWeight + "    sum_leaf_weights=" + aw);
        } else {
            LOG.info("Cooreect weights {} {}", lastNodeWeight, aw);
        }
        return lastNodeWeight;
    }

    public NormalizeResult updateTreeWeights(boolean weightsAlreadyNormalized, List<Particle> particles) {
        double neff = 0;
        if (!weightsAlreadyNormalized) {
            neff = normalize(particles);
        }
        resetTree(particles);
        propagateWeights(particles);
        return new NormalizeResult(new ArrayList<Double>(weights), neff);
    }

    public double normalize(List<Particle> particles) {
        //normalize the log weights
        double neff = 0;
        double gain = 1. / (obsSigmaGain * particles.size());
        double lmax = -Double.MAX_VALUE;
        for (Particle particle : particles) {
            lmax = particle.weight > lmax ? particle.weight : lmax;
        }

        weights.clear();
        double wcum = 0;
        neff = 0;
        for (Particle particle : particles) {
            double w = Math.exp(gain * (particle.weight - lmax));
//            double w = particle.weight;
            weights.add(w);
            wcum += w;
        }

        neff = 0;
        List<Double> temp = new ArrayList<Double>();
        for (Double weight1 : weights) {
            double weight;
            weight = weight1 / wcum;
            temp.add(weight);
            double w = weight;
            neff += w * w;
        }
        weights.clear();
        weights.addAll(temp);
        neff = 1. / neff;
        return neff;
    }

    public class NormalizeResult {
        private List<Double> weights;
        private double neff;

        public NormalizeResult(List<Double> weights, double neff) {
            this.weights = weights;
            this.neff = neff;
        }

        public List<Double> getWeights() {
            return weights;
        }

        public double getNeff() {
            return neff;
        }
    }
}
