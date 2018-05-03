package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.GFSConfiguration;

import java.util.Map;

public class ConfigurationBuilder {
    public static GFSConfiguration getConfiguration(Map conf) {
        GFSConfiguration configuration = new GFSConfiguration();

        configuration.setMapUpdateInterval(getDoubleProperty(conf, Constants.MAP_UPDATE_INTERVAL));
        configuration.setMaxRange(getDoubleProperty(conf, Constants.MAX_RANGE));
        configuration.setMaxURage(getDoubleProperty(conf, Constants.MAXURANGE));

        configuration.setGaussianSigma(getDoubleProperty(conf, Constants.SIGMA));
        configuration.setKernelSize(getIntProperty(conf, Constants.KERNELSIZE));
        configuration.setOptLinearDelta(getDoubleProperty(conf, Constants.LSTEP));
        configuration.setOptAngularDelta(getDoubleProperty(conf, Constants.ASTEP));
        configuration.setOptRecursiveIterations(getIntProperty(conf, Constants.ITERATIONS));
        configuration.setLikelihoodGain(getDoubleProperty(conf, Constants.OGAIN));
        configuration.setObsSigmaGain(getDoubleProperty(conf, Constants.OGAIN));

        configuration.setLikelihoodSigma(getDoubleProperty(conf, Constants.LSIGMA));

        configuration.setLikelihoodSkip(getIntProperty(conf, Constants.LSKIP));
        configuration.setSrr(getDoubleProperty(conf, Constants.SRR));
        configuration.setSrt(getDoubleProperty(conf, Constants.SRT));
        configuration.setStr(getDoubleProperty(conf, Constants.STR));
        configuration.setStt(getDoubleProperty(conf, Constants.STT));

        configuration.setLinearThresholdDistance(getDoubleProperty(conf, Constants.LINEAR_UPDATE));
        configuration.setAngularThresholdDistance(getDoubleProperty(conf, Constants.ANGULAR_UPDATE));
        configuration.setUpdatePeriod(getDoubleProperty(conf, Constants.TEMPORAL_UPDATE));
        configuration.setResampleThreshold(getDoubleProperty(conf, Constants.RESAMPLE_THRESHOLD));
        configuration.setNoOfParticles(getIntProperty(conf, Constants.PARTICLES));
        configuration.setXmin(getDoubleProperty(conf, Constants.XMIN));
        configuration.setXmax(getDoubleProperty(conf, Constants.XMAX));
        configuration.setYmin(getDoubleProperty(conf, Constants.YMIN));
        configuration.setYmax(getDoubleProperty(conf, Constants.YMAX));
        configuration.setDelta(getDoubleProperty(conf, Constants.DELTA));

        configuration.setLlsamplerange(getDoubleProperty(conf, Constants.LLSAMPLERANGE));
        configuration.setLlsamplestep(getDoubleProperty(conf, Constants.LLSAMPLESTEP));
        configuration.setLasamplerange(getDoubleProperty(conf, Constants.LASAMPLERANGE));
        configuration.setLasamplestep(getDoubleProperty(conf, Constants.LASAMPLESTEP));
        configuration.setOccThresh(getDoubleProperty(conf, Constants.OCC_THRESH));

        configuration.setMinimumScore(getDoubleProperty(conf, Constants.MIN_SCORE));
        configuration.setMaxCorrections(getIntProperty(conf, Constants.MAX_CORRECTIONS));

        return configuration;
    }

    public static int getIntProperty(Map conf, String property) {
        return (Integer) conf.get(property);
    }

    public static double getDoubleProperty(Map conf, String property) {
        return (Double) conf.get(property);
    }

    public static double getStringProperty(Map conf, String property) {
        return (Double) conf.get(property);
    }
}
