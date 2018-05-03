package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import edu.iu.dsc.tws.apps.slam.core.particlefilter.UniformResampler;
import edu.iu.dsc.tws.apps.slam.core.scanmatcher.ScanMatcher;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ReSampler {
    private Logger LOG = LoggerFactory.getLogger(ReSampler.class);

    protected double resampleThreshold;

    private boolean withMap;

    public ReSampler(double resampleThreshold) {
        this(resampleThreshold, true);
    }

    public ReSampler(double resampleThreshold, boolean withMap) {
        this.resampleThreshold = resampleThreshold;
        this.withMap = withMap;
    }

    public ReSampleResult resample(List<Particle> particles, double neff, double[] plainReading,
                            int adaptSize, RangeReading reading, List<Double> weights) {
        boolean hasResampled = false;
        List<TNode> oldGeneration = new ArrayList<TNode>();
        for (Particle m_particle : particles) {
            oldGeneration.add(m_particle.node);
        }

        if (neff < resampleThreshold * particles.size()) {
            LOG.info("ReSampled: neff < resampleThreshold * particles.size() and resampling {} < {}", neff, resampleThreshold * particles.size());
            List<Integer> indexes = UniformResampler.resampleIndexes(weights, adaptSize);
            StringBuilder m_outputStream = new StringBuilder("RESAMPLE ").append(indexes.size());
            for (Integer it : indexes) {
                m_outputStream.append(it).append(" ");
            }
            LOG.info(m_outputStream.toString());


            //begin building tree
            List<Particle> temp = new ArrayList<Particle>();
            int j = 0;
            //this is for deleteing the particles which have been resampled away.
            List<Integer> deletedParticles = new ArrayList<Integer>();

            for (Integer ind : indexes) {
                while (j < ind) {
                    deletedParticles.add(j);
                    j++;
                }
                if (j == ind) {
                    j++;
                }
                Particle p;
                p = new Particle(particles.get(ind), withMap);

                TNode node;
                TNode oldNode = oldGeneration.get(ind);
                node = new TNode(p.pose, 0, oldNode, 0);
                node.reading = reading;

                temp.add(p);
                p.node = node;
                p.previousIndex = ind;
            }
            while (j < indexes.size()) {
                deletedParticles.add(j);
                j++;
            }

            for (Integer deletedParticle : deletedParticles) {
                if (deletedParticle < particles.size()) {
                    particles.get(deletedParticle).node = null;
                }
            }

            particles.clear();
            LOG.debug("Copying Particles and  Registering  scans...");
            for (Particle it : temp) {
                it.setWeight(0);
                particles.add(it);
            }
            hasResampled = true;

            return new ReSampleResult(true, indexes);
        } else {
            LOG.info("NOT ReSampled: neff > resampleThreshold * particles.size() and resampling {} > {}", neff, resampleThreshold * particles.size());
            return new ReSampleResult(false, null);
        }
    }

    public static class ReSampleResult {
        private boolean reSampled;

        private List<Integer> indexes;

        public ReSampleResult(boolean reSampled, List<Integer> indexes) {
            this.reSampled = reSampled;
            this.indexes = indexes;
        }

        public boolean isReSampled() {
            return reSampled;
        }

        public List<Integer> getIndexes() {
            return indexes;
        }
    }
}
