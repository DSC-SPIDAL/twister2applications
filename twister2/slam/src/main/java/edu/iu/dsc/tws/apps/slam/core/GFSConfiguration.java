package edu.iu.dsc.tws.apps.slam.core;

public class GFSConfiguration {
    protected int beams;

    protected int noOfParticles;

    protected double minimumScore;

    protected double resampleThreshold;

    protected double linearDistance, angularDistance;

    protected double srr;
    protected double srt;
    protected double str;
    protected double stt;

    protected double xmin;
    protected double ymin;
    protected double xmax;
    protected double ymax;

    protected double delta;
    protected double regScore;
    protected double critScore;
    protected double maxMove;
    protected double linearThresholdDistance;
    protected double angularThresholdDistance;
    protected double updatePeriod;
    protected double obsSigmaGain;

    protected double gaussianSigma;
    protected double likelihoodGain;
    protected double likelihoodSigma;
    protected int kernelSize;
    protected double optAngularDelta;
    protected double optLinearDelta;
    protected int optRecursiveIterations = 3;
    protected int likelihoodSkip;
    protected double llsamplerange;
    protected double lasamplerange;
    protected double llsamplestep;
    protected double lasamplestep;
    protected boolean generateMap;
    protected double enlargeStep = 10;
    protected double fullnessThreshold = 0.1;
    protected double angularOdometryReliability = 0;
    protected double linearOdometryReliability = 0;
    protected double freeCellRatio = Math.sqrt(2.0);
    protected int initialBeamsSkip = 0;

    protected double maxURage;
    protected double maxRange;

    protected double mapUpdateInterval = 5.0;

    protected double occThresh = .3;
    private int maxCorrections;

    public double getOccThresh() {
        return occThresh;
    }

    public void setOccThresh(double occThresh) {
        this.occThresh = occThresh;
    }

    public double getMaxURage() {
        return maxURage;
    }

    public double getMaxRange() {
        return maxRange;
    }

    public void setMaxURage(double maxURage) {
        this.maxURage = maxURage;
    }

    public void setMaxRange(double maxRange) {
        this.maxRange = maxRange;
    }

    public double getLikelihoodGain() {
        return likelihoodGain;
    }

    public void setLikelihoodGain(double likelihoodGain) {
        this.likelihoodGain = likelihoodGain;
    }

    public int getNoOfParticles() {
        return noOfParticles;
    }

    public void setNoOfParticles(int noOfParticles) {
        this.noOfParticles = noOfParticles;
    }

    public double getUpdatePeriod() {
        return updatePeriod;
    }

    public void setUpdatePeriod(double updatePeriod) {
        this.updatePeriod = updatePeriod;
    }

    public double getSrr() {
        return srr;
    }

    public double getSrt() {
        return srt;
    }

    public double getStr() {
        return str;
    }

    public double getStt() {
        return stt;
    }

    public void setSrr(double srr) {
        this.srr = srr;
    }

    public void setSrt(double srt) {
        this.srt = srt;
    }

    public void setStr(double str) {
        this.str = str;
    }

    public void setStt(double stt) {
        this.stt = stt;
    }

    public int getBeams() {
        return beams;
    }

    public double getMinimumScore() {
        return minimumScore;
    }

    public double getResampleThreshold() {
        return resampleThreshold;
    }

    public double getLinearDistance() {
        return linearDistance;
    }

    public double getAngularDistance() {
        return angularDistance;
    }


    public double getXmin() {
        return xmin;
    }

    public double getYmin() {
        return ymin;
    }

    public double getXmax() {
        return xmax;
    }

    public double getYmax() {
        return ymax;
    }

    public double getDelta() {
        return delta;
    }

    public double getRegScore() {
        return regScore;
    }

    public double getCritScore() {
        return critScore;
    }

    public double getMaxMove() {
        return maxMove;
    }

    public double getLinearThresholdDistance() {
        return linearThresholdDistance;
    }

    public double getAngularThresholdDistance() {
        return angularThresholdDistance;
    }

    public double getObsSigmaGain() {
        return obsSigmaGain;
    }

    public double getGaussianSigma() {
        return gaussianSigma;
    }

    public double getLikelihoodSigma() {
        return likelihoodSigma;
    }

    public int getKernelSize() {
        return kernelSize;
    }

    public double getOptAngularDelta() {
        return optAngularDelta;
    }

    public double getOptLinearDelta() {
        return optLinearDelta;
    }

    public int getOptRecursiveIterations() {
        return optRecursiveIterations;
    }

    public int getLikelihoodSkip() {
        return likelihoodSkip;
    }

    public double getLlsamplerange() {
        return llsamplerange;
    }

    public double getLasamplerange() {
        return lasamplerange;
    }

    public double getLlsamplestep() {
        return llsamplestep;
    }

    public double getLasamplestep() {
        return lasamplestep;
    }

    public boolean isGenerateMap() {
        return generateMap;
    }

    public double getEnlargeStep() {
        return enlargeStep;
    }

    public double getFullnessThreshold() {
        return fullnessThreshold;
    }

    public double getAngularOdometryReliability() {
        return angularOdometryReliability;
    }

    public double getLinearOdometryReliability() {
        return linearOdometryReliability;
    }

    public double getFreeCellRatio() {
        return freeCellRatio;
    }

    public int getInitialBeamsSkip() {
        return initialBeamsSkip;
    }

    public void setBeams(int beams) {
        this.beams = beams;
    }

    public void setMinimumScore(double minimumScore) {
        this.minimumScore = minimumScore;
    }

    public void setResampleThreshold(double resampleThreshold) {
        this.resampleThreshold = resampleThreshold;
    }

    public void setLinearDistance(double linearDistance) {
        this.linearDistance = linearDistance;
    }

    public void setAngularDistance(double angularDistance) {
        this.angularDistance = angularDistance;
    }

    public void setXmin(double xmin) {
        this.xmin = xmin;
    }

    public void setYmin(double ymin) {
        this.ymin = ymin;
    }

    public void setXmax(double xmax) {
        this.xmax = xmax;
    }

    public void setYmax(double ymax) {
        this.ymax = ymax;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    public void setRegScore(double regScore) {
        this.regScore = regScore;
    }

    public void setCritScore(double critScore) {
        this.critScore = critScore;
    }

    public void setMaxMove(double maxMove) {
        this.maxMove = maxMove;
    }

    public void setLinearThresholdDistance(double linearThresholdDistance) {
        this.linearThresholdDistance = linearThresholdDistance;
    }

    public void setAngularThresholdDistance(double angularThresholdDistance) {
        this.angularThresholdDistance = angularThresholdDistance;
    }

    public void setObsSigmaGain(double obsSigmaGain) {
        this.obsSigmaGain = obsSigmaGain;
    }

    public void setGaussianSigma(double gaussianSigma) {
        this.gaussianSigma = gaussianSigma;
    }

    public void setLikelihoodSigma(double likelihoodSigma) {
        this.likelihoodSigma = likelihoodSigma;
    }

    public void setKernelSize(int kernelSize) {
        this.kernelSize = kernelSize;
    }

    public void setOptAngularDelta(double optAngularDelta) {
        this.optAngularDelta = optAngularDelta;
    }

    public void setOptLinearDelta(double optLinearDelta) {
        this.optLinearDelta = optLinearDelta;
    }

    public void setOptRecursiveIterations(int optRecursiveIterations) {
        this.optRecursiveIterations = optRecursiveIterations;
    }

    public void setLikelihoodSkip(int likelihoodSkip) {
        this.likelihoodSkip = likelihoodSkip;
    }

    public void setLlsamplerange(double llsamplerange) {
        this.llsamplerange = llsamplerange;
    }

    public void setLasamplerange(double lasamplerange) {
        this.lasamplerange = lasamplerange;
    }

    public void setLlsamplestep(double llsamplestep) {
        this.llsamplestep = llsamplestep;
    }

    public void setLasamplestep(double lasamplestep) {
        this.lasamplestep = lasamplestep;
    }

    public void setGenerateMap(boolean generateMap) {
        this.generateMap = generateMap;
    }

    public void setEnlargeStep(double enlargeStep) {
        this.enlargeStep = enlargeStep;
    }

    public void setFullnessThreshold(double fullnessThreshold) {
        this.fullnessThreshold = fullnessThreshold;
    }

    public void setAngularOdometryReliability(double angularOdometryReliability) {
        this.angularOdometryReliability = angularOdometryReliability;
    }

    public void setLinearOdometryReliability(double linearOdometryReliability) {
        this.linearOdometryReliability = linearOdometryReliability;
    }

    public void setFreeCellRatio(double freeCellRatio) {
        this.freeCellRatio = freeCellRatio;
    }

    public void setInitialBeamsSkip(int initialBeamsSkip) {
        this.initialBeamsSkip = initialBeamsSkip;
    }

    public double getMapUpdateInterval() {
        return mapUpdateInterval;
    }

    public void setMapUpdateInterval(double mapUpdateInterval) {
        this.mapUpdateInterval = mapUpdateInterval;
    }

    public void setMaxCorrections(int maxCorrections) {
        this.maxCorrections = maxCorrections;
    }

    public int getMaxCorrections() {
        return maxCorrections;
    }
}
