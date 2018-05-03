package edu.iu.dsc.tws.apps.slam.core.scanmatcher;

import edu.iu.dsc.tws.apps.slam.core.grid.IGMap;
import edu.iu.dsc.tws.apps.slam.core.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ScanMatcher {
    private static Logger LOG = LoggerFactory.getLogger(ScanMatcher.class);

    public static final int LASER_MAXBEAMS = 2048;
    public static final double nullLikelihood = -.5;

    double laserMaxRange;
    double usableRange;
    double gaussianSigma;
    double likelihoodSigma;
    int kernelSize;
    double optAngularDelta;
    double optLinearDelta;
    int optRecursiveIterations = 3;
    int likelihoodSkip;
    double llsamplerange;
    double lasamplerange;
    double llsamplestep;
    double lasamplestep;
    boolean generateMap;
    double enlargeStep = 10;
    DoubleOrientedPoint laserPose;
    double fullnessThreshold = 0.2;
    double angularOdometryReliability = 0;
    double linearOdometryReliability = 0;
    double freeCellRatio = Math.sqrt(2.0);
    int initialBeamsSkip = 0;
    int maxCorrections = 200;

    boolean m_activeAreaComputed = false;

    enum Move {Front, Back, Left, Right, TurnLeft, TurnRight, Done}

    /**
     * laser parameters
     */
    int laserBeams;
    double laserAngles[] = new double[LASER_MAXBEAMS];

    public void setLaserMaxRange(double m_laserMaxRange) {
        this.laserMaxRange = m_laserMaxRange;
    }

    public void setUsableRange(double m_usableRange) {
        this.usableRange = m_usableRange;
    }

    public void setGaussianSigma(double m_gaussianSigma) {
        this.gaussianSigma = m_gaussianSigma;
    }

    public void setLikelihoodSigma(double m_likelihoodSigma) {
        this.likelihoodSigma = m_likelihoodSigma;
    }

    public void setKernelSize(int m_kernelSize) {
        this.kernelSize = m_kernelSize;
    }

    public void setOptAngularDelta(double m_optAngularDelta) {
        this.optAngularDelta = m_optAngularDelta;
    }

    public void setOptLinearDelta(double m_optLinearDelta) {
        this.optLinearDelta = m_optLinearDelta;
    }

    public void setOptRecursiveIterations(int m_optRecursiveIterations) {
        this.optRecursiveIterations = m_optRecursiveIterations;
    }

    public void setLikelihoodSkip(int m_likelihoodSkip) {
        this.likelihoodSkip = m_likelihoodSkip;
    }

    public void setLLSamplerange(double m_llsamplerange) {
        this.llsamplerange = m_llsamplerange;
    }

    public void setLSSamplerange(double m_lasamplerange) {
        this.lasamplerange = m_lasamplerange;
    }

    public void setLLSamplestep(double m_llsamplestep) {
        this.llsamplestep = m_llsamplestep;
    }

    public void setLASamplestep(double m_lasamplestep) {
        this.lasamplestep = m_lasamplestep;
    }

    public void setgenerateMap(boolean m_generateMap) {
        this.generateMap = m_generateMap;
    }

    public void setenlargeStep(double m_enlargeStep) {
        this.enlargeStep = m_enlargeStep;
    }

    public void setlaserPose(DoubleOrientedPoint m_laserPose) {
        this.laserPose = m_laserPose;
    }

    public void setfullnessThreshold(double m_fullnessThreshold) {
        this.fullnessThreshold = m_fullnessThreshold;
    }

    public void setangularOdometryReliability(double m_angularOdometryReliability) {
        this.angularOdometryReliability = m_angularOdometryReliability;
    }

    public void setlinearOdometryReliability(double m_linearOdometryReliability) {
        this.linearOdometryReliability = m_linearOdometryReliability;
    }

    public void setfreeCellRatio(double m_freeCellRatio) {
        this.freeCellRatio = m_freeCellRatio;
    }

    public void setinitialBeamsSkip(int m_initialBeamsSkip) {
        this.initialBeamsSkip = m_initialBeamsSkip;
    }

    public void setactiveAreaComputed(boolean m_activeAreaComputed) {
        this.m_activeAreaComputed = m_activeAreaComputed;
    }

    public void setMaxCorrections(int maxCorrections) {
        this.maxCorrections = maxCorrections;
    }

    public void setMatchingParameters
            (double urange, double range, double sigma, int kernsize, double lopt, double aopt, int iterations, double likelihoodSigma, int likelihoodSkip) {
        usableRange = urange;
        laserMaxRange = range;
        kernelSize = kernsize;
        optLinearDelta = lopt;
        optAngularDelta = aopt;
        optRecursiveIterations = iterations;
        gaussianSigma = sigma;
        this.likelihoodSigma = likelihoodSigma;
        this.likelihoodSkip = likelihoodSkip;
    }

    public void computeActiveArea(IGMap map, DoubleOrientedPoint p, double[] readings) {
        if (m_activeAreaComputed)
            return;
        //Set<IntPoint> activeArea = new HashSet<IntPoint>();
        DoubleOrientedPoint lp = new DoubleOrientedPoint(p.x, p.y, p.theta);
        lp.x += Math.cos(p.theta) * laserPose.x - Math.sin(p.theta) * laserPose.y;
        lp.y += Math.sin(p.theta) * laserPose.x + Math.cos(p.theta) * laserPose.y;
        lp.theta += laserPose.theta;
//        IntPoint p0 = map.world2map(lp);

        DoublePoint min = map.map2world(new IntPoint(0, 0));
        DoublePoint max = map.map2world(new IntPoint(map.getMapSizeX() - 1, map.getMapSizeY() - 1));

        if (lp.x < min.x) min.x = lp.x;
        if (lp.y < min.y) min.y = lp.y;
        if (lp.x > max.x) max.x = lp.x;
        if (lp.y > max.y) max.y = lp.y;
        int readingIndex;
        int angleIndex = initialBeamsSkip;
        for (readingIndex = initialBeamsSkip; readingIndex < laserBeams; readingIndex++, angleIndex++) {
            double r = readings[readingIndex];
            double angle = laserAngles[angleIndex];

            if (r > laserMaxRange || r == 0.0 || r > Double.MAX_VALUE) continue;
            double d = r > usableRange ? usableRange : r;
//            DoublePoint phit = new DoublePoint(lp.x, lp.y);
            double phitx = lp.x, phity = lp.y;
            phitx += d * Math.cos(lp.theta + angle);
            phity += d * Math.sin(lp.theta + angle);
            if (phitx < min.x) min.x = phitx;
            if (phity < min.y) min.y = phity;
            if (phitx > max.x) max.x = phitx;
            if (phity > max.y) max.y = phity;
        }

        if (!map.isInsideD(min) || !map.isInsideD(max)) {
            DoublePoint lmin = map.map2world(new IntPoint(0, 0));
            DoublePoint lmax = map.map2world(new IntPoint(map.getMapSizeX() - 1, map.getMapSizeY() - 1));
            min.x = (min.x >= lmin.x) ? lmin.x : min.x - enlargeStep;
            max.x = (max.x <= lmax.x) ? lmax.x : max.x + enlargeStep;
            min.y = (min.y >= lmin.y) ? lmin.y : min.y - enlargeStep;
            max.y = (max.y <= lmax.y) ? lmax.y : max.y + enlargeStep;
            map.resize(min.x, min.y, max.x, max.y);
        }

//        readingIndex = initialBeamsSkip;
//        angleIndex = initialBeamsSkip;
//        for (readingIndex = initialBeamsSkip; readingIndex < laserBeams; readingIndex++, angleIndex++) {
//            if (generateMap) {
//                double d = readings[readingIndex];
//                if (d > laserMaxRange || d == 0.0 || d > Double.MAX_VALUE)
//                    continue;
//                if (d > usableRange)
//                    d = usableRange;
//
//                DoublePoint phit = new DoublePoint(d * Math.cos(lp.theta + laserAngles[angleIndex]) + lp.x, d * Math.sin(lp.theta + laserAngles[angleIndex]) + lp.y);
//                p0 = map.world2map(lp);
//                IntPoint p1 = map.world2map(phit);
//
//                d += map.getDelta();
//                GridLineTraversalLine line = new GridLineTraversalLine();
//                line.points = m_linePoints;
//                GridLineTraversalLine.gridLine(p0, p1, line);
//                for (int i = 0; i < line.numPoints - 1; i++) {
//                    IntPoint patch = (m_linePoints[i]);
//                    activeArea.add(patch);
//                }
//                if (d <= usableRange) {
//                    IntPoint patch = (p1);
//                    activeArea.add(patch);
//                }
//            } else {
//                double r = readings[readingIndex];
//                double angle = laserAngles[angleIndex];
//                if (readings[readingIndex] > laserMaxRange || readings[readingIndex] > usableRange) {
//                    continue;
//                }
////                DoublePoint phit = new DoublePoint(lp.x, lp.y);
//                double phitx = lp.x;
//                double phity = lp.y;
//                phitx += r * Math.cos(lp.theta + angle);
//                phity += r * Math.sin(lp.theta + angle);
//
//                IntPoint p1 = map.world2map(phitx, phity);
//                assert (p1.x >= 0 && p1.y >= 0);
//                IntPoint cp = (p1);
//                assert (cp.x >= 0 && cp.y >= 0);
//                activeArea.add(cp);
//            }
//        }
        //this allocates the unallocated cells in the active area of the map
        //map.setActiveArea(activeArea, false);
        m_activeAreaComputed = true;
    }

    private static int count = 0;


    private IntPoint m_linePoints [] = new IntPoint[20000];

    public double registerScan(IGMap map, DoubleOrientedPoint p, double[] readings) {
        if (!m_activeAreaComputed)
            computeActiveArea(map, p, readings);

        //this operation replicates the cells that will be changed in the registration operation
        // map.allocActiveArea();

        DoubleOrientedPoint lp = new DoubleOrientedPoint(p.x, p.y, p.theta);
        lp.x += Math.cos(p.theta) * laserPose.x - Math.sin(p.theta) * laserPose.y;
        lp.y += Math.sin(p.theta) * laserPose.x + Math.cos(p.theta) * laserPose.y;
        lp.theta += laserPose.theta;
        IntPoint p0 = map.world2map(lp);
        int readingIndex;
        int angleIndex = initialBeamsSkip;
        double esum = 0;
        for (readingIndex = initialBeamsSkip; readingIndex < laserBeams; readingIndex++, angleIndex++) {
            if (generateMap) {
                double d = readings[readingIndex];
                if (d >= laserMaxRange)
                    continue;
                if (d >= usableRange)
                    d = usableRange;
                DoublePoint phit = new DoublePoint(d * Math.cos(Utils.theta(lp.theta + laserAngles[angleIndex])) + lp.x, d * Math.sin(Utils.theta(lp.theta + laserAngles[angleIndex])) + lp.y);
                IntPoint p1 = map.world2map(phit);

                d += map.getDelta();
                GridLineTraversalLine line = new GridLineTraversalLine();
                line.points = m_linePoints;
                GridLineTraversalLine.gridLine(p0, p1, line);
                for (int i = 0; i < line.numPoints - 1; i++) {
                    PointAccumulator pa = (PointAccumulator) map.cell(line.points[i], false);
                    if (pa != null) {
                        double e = -pa.entropy();
                        pa.update(false, new DoublePoint(0.0, 0.0));
                        e += pa.entropy();
                        esum += e;
                    }
                }
                if (d < usableRange) {
                    PointAccumulator pa = (PointAccumulator) map.cell(p1, false);
                    if (pa != null) {
                        double e = -pa.entropy();
                        pa.update(true, phit);
                        e += pa.entropy();
                        esum += e;
                    }
                }


            } else {
                double r = readings[readingIndex];
                if (r >= laserMaxRange || r >= usableRange || r == 0) {
                    continue;
                }
//                DoublePoint phit = new DoublePoint(lp.x, lp.y);
                double phitx = lp.x;
                double phity = lp.y;
                phitx += r * Math.cos(Utils.theta(lp.theta + laserAngles[angleIndex]));
                phity += r * Math.sin(Utils.theta(lp.theta + laserAngles[angleIndex]));
                IntPoint p1 = map.world2map(phitx, phity);
                PointAccumulator pa = (PointAccumulator) map.cell(p1, false);
                if (pa != null) {
                    pa.update(true, phitx, phity);
                }
            }
        }
        return esum;
    }

    public double icpOptimize(DoubleOrientedPoint pnew, IGMap map, DoubleOrientedPoint init, double[] readings) {
        double currentScore;
        double sc = score(map, init, readings);
        DoubleOrientedPoint start = init;
        pnew.x = init.x;
        pnew.y = init.y;
        pnew.theta = init.theta;
        int iterations = 0;
        do {
            currentScore = sc;
            sc = icpStep(pnew, map, start, readings);
            start = pnew;
            iterations++;
        } while (sc > currentScore);
        return currentScore;
    }

    public double optimize(DoubleOrientedPoint _mean, Covariance3 _cov, IGMap map, DoubleOrientedPoint init, double[] readings) {
        List<ScoredMove> moveList = new ArrayList<ScoredMove>();
        double bestScore = -1;
        DoubleOrientedPoint currentPose = init;
        ScoredMove sm = new ScoredMove(currentPose, 0, 0);
        LikeliHoodAndScore ls = likelihoodAndScore(map, currentPose, readings);
        sm.likelihood = ls.l;
        sm.score = ls.s;
        double currentScore = sm.score;
        moveList.add(sm);
        double adelta = optAngularDelta, ldelta = optLinearDelta;
        int refinement = 0;

        do {
            if (bestScore >= currentScore) {
                refinement++;
                adelta *= .5;
                ldelta *= .5;
            }
            bestScore = currentScore;
            DoubleOrientedPoint bestLocalPose = currentPose;
            DoubleOrientedPoint localPose = currentPose;

            Move move = Move.Front;
            do {
                localPose = currentPose;
                switch (move) {
                    case Front:
                        localPose.x += ldelta;
                        move = Move.Back;
                        break;
                    case Back:
                        localPose.x -= ldelta;
                        move = Move.Left;
                        break;
                    case Left:
                        localPose.y -= ldelta;
                        move = Move.Right;
                        break;
                    case Right:
                        localPose.y += ldelta;
                        move = Move.TurnLeft;
                        break;
                    case TurnLeft:
                        localPose.theta += adelta;
                        move = Move.TurnRight;
                        break;
                    case TurnRight:
                        localPose.theta -= adelta;
                        move = Move.Done;
                        break;
                    default:
                }
                double localScore = 0, localLikelihood = 0;
                //update the score
                ls = likelihoodAndScore(map, localPose, readings);
                localLikelihood = ls.s;
                localLikelihood = ls.l;
                if (localScore > currentScore) {
                    currentScore = localScore;
                    bestLocalPose = localPose;
                }
                sm.score = localScore;
                sm.likelihood = localLikelihood;
                sm.pose = localPose;
                moveList.add(sm);
                //update the move list
            } while (move != Move.Done);
            currentPose = bestLocalPose;
            //here we look for the best move;
        } while (currentScore > bestScore || refinement < optRecursiveIterations);

        //normalize the likelihood
        double lmin = 1e9;
        double lmax = -1e9;
        for (ScoredMove it : moveList) {
            lmin = it.likelihood < lmin ? it.likelihood : lmin;
            lmax = it.likelihood > lmax ? it.likelihood : lmax;
        }
        for (ScoredMove it : moveList) {
            it.likelihood = Math.exp(it.likelihood - lmax);
        }
        //compute the mean
        DoubleOrientedPoint mean = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        double lacc = 0;
        for (ScoredMove it : moveList) {
            mean = DoubleOrientedPoint.plus(mean, DoubleOrientedPoint.mulN(it.pose, it.likelihood));
            lacc += it.likelihood;
        }
        mean = DoubleOrientedPoint.mulN(mean, (1. / lacc));
        Covariance3 cov = new Covariance3(0., 0., 0., 0., 0., 0.);
        for (ScoredMove it : moveList) {
            DoubleOrientedPoint delta = DoubleOrientedPoint.minus(it.pose, mean);
            delta.theta = Math.atan2(Math.sin(delta.theta), Math.cos(delta.theta));
            cov.xx += delta.x * delta.x * it.likelihood;
            cov.yy += delta.y * delta.y * it.likelihood;
            cov.tt += delta.theta * delta.theta * it.likelihood;
            cov.xy += delta.x * delta.y * it.likelihood;
            cov.xt += delta.x * delta.theta * it.likelihood;
            cov.yt += delta.y * delta.theta * it.likelihood;
        }
        cov.xx /= lacc;
        cov.xy /= lacc;
        cov.xt /= lacc;
        cov.yy /= lacc;
        cov.yt /= lacc;
        cov.tt /= lacc;

        _mean = currentPose;
        _cov = cov;
        return bestScore;
    }

    private class Pair {
        double one;
        double two;

        private Pair(double one, double two) {
            this.one = one;
            this.two = two;
        }
    }

    public double optimize(DoubleOrientedPoint pnew, IGMap map, DoubleOrientedPoint init, double[] readings) {
        double bestScore = -1;
        DoubleOrientedPoint currentPose = new DoubleOrientedPoint(init.x, init.y, init.theta);
        LikeliHoodAndScore likeliHoodAndScore = likelihoodAndScore(map, currentPose, readings);
//        double currentScore = score(map, currentPose, readings);
        double currentScore = likeliHoodAndScore.s;
        double adelta = optAngularDelta, ldelta = optLinearDelta;
        int refinement = 0;

        int totalCorrections = 0;
//        int []countRefinement = new int[7];
        int count = 0;
//        List<ArrayList<Pair>> currentScores = new ArrayList<ArrayList<Pair>>();
//        List<ArrayList<DoubleOrientedPoint>> points = new ArrayList<ArrayList<DoubleOrientedPoint>>();
//        ArrayList<DoubleOrientedPoint> p = new ArrayList<DoubleOrientedPoint>();
//        ArrayList<Pair> e = new ArrayList<Pair>();
//        currentScores.add(e);
//        points.add(p);
        do {
            if (bestScore >= currentScore || (Double.isNaN(bestScore) && Double.isNaN(currentScore))) {
//                countRefinement[refinement] = count;
                count = 0;
                refinement++;
                adelta *= .5;
                ldelta *= .5;
//                e = new ArrayList<Pair>();
//                currentScores.add(e);
//                p=  new ArrayList<DoubleOrientedPoint>();
//                points.add(p);
            }
            bestScore = currentScore;
            DoubleOrientedPoint bestLocalPose = new DoubleOrientedPoint(currentPose.x, currentPose.y, currentPose.theta);
            DoubleOrientedPoint localPose;

            Move move = Move.Front;
            do {
                localPose = new DoubleOrientedPoint(currentPose.x, currentPose.y, currentPose.theta);
                switch (move) {
                    case Front:
                        localPose.x += ldelta;
                        move = Move.Back;
                        break;
                    case Back:
                        localPose.x -=  ldelta;
                        move = Move.Left;
                        break;
                    case Left:
                        localPose.y -= ldelta;
                        move = Move.Right;
                        break;
                    case Right:
                        localPose.y += ldelta;
                        move = Move.TurnLeft;
                        break;
                    case TurnLeft:
                        localPose.theta += adelta;
                        move = Move.TurnRight;
                        break;
                    case TurnRight:
                        localPose.theta -= adelta;
                        move = Move.Done;
                        break;
                    default:
                }

                localPose.theta = Utils.theta(localPose.theta);

                double odo_gain = 1;
                if (angularOdometryReliability > 0.) {
                    double dth = Utils.theta(init.theta - localPose.theta);
                    dth = Math.atan2(Math.sin(dth), Math.cos(dth));
                    dth *= dth;
                    odo_gain *= Math.exp(-angularOdometryReliability * dth);
                }
                if (linearOdometryReliability > 0.) {
                    double dx = init.x - localPose.x;
                    double dy = init.y - localPose.y;
                    double drho = dx * dx + dy * dy;
                    odo_gain *= Math.exp(-linearOdometryReliability * drho);
                }

                likeliHoodAndScore = likelihoodAndScore(map, localPose, readings);
                count++;
                totalCorrections++;
                if (totalCorrections >= maxCorrections) {
                    move = Move.Done;
                }
//                if (count == 100) {
//                    move = Move.Done;
//                }
//                double localScore = odo_gain * score(map, localPose, readings);
                double localScore = odo_gain * likeliHoodAndScore.s;
                if (localScore > currentScore) {
                    currentScore = localScore;
                    bestLocalPose = new DoubleOrientedPoint(localPose.x, localPose.y, localPose.theta);
                }
            } while (move != Move.Done);
            currentPose = new DoubleOrientedPoint(bestLocalPose.x, bestLocalPose.y, bestLocalPose.theta);
            // p.add(currentPose);
            // e.add(new Pair(currentScore, bestScore));
        } while ((currentScore > bestScore || refinement <= optRecursiveIterations) && totalCorrections < maxCorrections);
        pnew.x = currentPose.x;
        pnew.y = currentPose.y;
//        System.out.println(totalCorrections);
        pnew.theta = currentPose.theta;
        // printCorrectionInfo(refinement, countRefinement, currentScores, points);
        return bestScore;
    }

    private void printCorrectionInfo(int refinement, int[] countRefinement,
                                     List<ArrayList<Pair>> currentScores,
                                     List<ArrayList<DoubleOrientedPoint>> points) {
        ArrayList<Pair> e;
        ArrayList<DoubleOrientedPoint> p;
        String s = "";
        int c  = 0;
        for (int i = 0; i < countRefinement.length; i++) {
            s += countRefinement[i] + " ";
            c += countRefinement[i];
        }
        if  (c > 200) {
            for (int i = 0; i < currentScores.size(); i++) {
                e = currentScores.get(i);
                p = points.get(i);
                String pt = "" + p.size() + ":";
                String r = "" + e.size() + " :";
                for (int j = 0; j < p.size(); j++) {
                    Pair pr = e.get(j);
                    DoubleOrientedPoint point = p.get(j);
                    r += pr.one + ", " + pr.two + "  ";
                    pt += "(" + point.x + " " + point.y + " " + point.theta +")";
                }
                System.out.println(r);
                System.out.println(pt);
                System.out.println("*************");
            }
        }
        System.out.println("Iteration count: " + c + ", " + s + " refinement: " + refinement);
    }

    public void setLaserParameters(int beams, double[] angles, DoubleOrientedPoint lpose) {
        laserPose = lpose;
        laserBeams = beams;
        laserAngles = new double[beams];
        System.arraycopy(angles, 0, laserAngles, 0, angles.length);
    }

    public void invalidateActiveArea() {
        m_activeAreaComputed = false;
    }

    public LikeliHood likelihood
            (IGMap map, DoubleOrientedPoint p, double[] readings) {
        List<ScoredMove> moveList = new ArrayList<ScoredMove>();

        for (double xx = -llsamplerange; xx <= llsamplerange; xx += llsamplestep) {
            for (double yy = -llsamplerange; yy <= llsamplerange; yy += llsamplestep) {
                for (double tt = -lasamplerange; tt <= lasamplerange; tt += lasamplestep) {
                    DoubleOrientedPoint rp = new DoubleOrientedPoint(p.x, p.y, p.theta);
                    rp.x += xx;
                    rp.y += yy;
                    rp.theta += tt;

                    ScoredMove sm = new ScoredMove();
                    sm.pose = rp;

                    LikeliHoodAndScore ls = likelihoodAndScore(map, rp, readings);
                    sm.score = ls.s;
                    sm.likelihood = ls.l;

                    moveList.add(sm);
                }
            }
        }

        //normalize the likelihood
        double lmax = -1e9;
        double lcum = 0;
        for (ScoredMove it : moveList) {
            lmax = it.likelihood > lmax ? it.likelihood : lmax;
        }
        for (ScoredMove it : moveList) {
            lcum += Math.exp(it.likelihood - lmax);
            it.likelihood = Math.exp(it.likelihood - lmax);
        }

        DoubleOrientedPoint mean = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        for (ScoredMove it : moveList) {
            double x = mean.x + it.pose.x * it.likelihood;
            double y = mean.y + it.pose.y * it.likelihood;
            double theta = mean.theta + it.pose.theta * it.likelihood;
            mean.x = x;
            mean.y = y;
            mean.theta = theta;
        }
        mean.x = mean.x * (1. / lcum);
        mean.y = mean.y * (1. / lcum);
        mean.theta = mean.theta * (1. / lcum);

        Covariance3 cov = new Covariance3(0., 0., 0., 0., 0., 0.);
        for (ScoredMove it : moveList) {
            DoubleOrientedPoint delta = DoubleOrientedPoint.minus(it.pose, mean);
            delta.theta = Math.atan2(Math.sin(delta.theta), Math.cos(delta.theta));
            cov.xx += delta.x * delta.x * it.likelihood;
            cov.yy += delta.y * delta.y * it.likelihood;
            cov.tt += delta.theta * delta.theta * it.likelihood;
            cov.xy += delta.x * delta.y * it.likelihood;
            cov.xt += delta.x * delta.theta * it.likelihood;
            cov.yt += delta.y * delta.theta * it.likelihood;
        }
        cov.xx /= lcum;
        cov.xy /= lcum;
        cov.xt /= lcum;
        cov.yy /= lcum;
        cov.yt /= lcum;
        cov.tt /= lcum;

        return new LikeliHood(lmax, mean, cov, Math.log(lcum));
    }

    public class LikeliHoodAndScore {
        public double s;
        public double l;
        public double c;

        public LikeliHoodAndScore(double s, double l, double c) {
            this.s = s;
            this.l = l;
            this.c = c;
        }
    }

    public LikeliHoodAndScore likelihoodAndScore(IGMap map, DoubleOrientedPoint p, double[] readings) {
        double l = 0;
        double s = 0;
        int angleIndex = initialBeamsSkip;
        DoubleOrientedPoint lp = new DoubleOrientedPoint(p.x, p.y, p.theta);

        lp.x += Math.cos(p.theta) * laserPose.x - Math.sin(p.theta) * laserPose.y;
        lp.y += Math.sin(p.theta) * laserPose.x + Math.cos(p.theta) * laserPose.y;
        lp.theta += laserPose.theta;
        double noHit = nullLikelihood / (likelihoodSigma);
        int skip = 0;
        int c = 0;
        PointAccumulator emptyPointAccumulator = new PointAccumulator();
        double freeDelta = map.getDelta() * freeCellRatio;
        for (int rIndex = initialBeamsSkip; rIndex < readings.length; rIndex++, angleIndex++) {
            skip++;
            skip = skip > likelihoodSkip ? 0 : skip;
            if (readings[rIndex] > usableRange) {
                continue;
            }
            if (skip != 0) {
                continue;
            }
            double phitx = lp.x;
            double phity = lp.y;
            phitx += readings[rIndex] * Math.cos(Utils.theta(lp.theta + laserAngles[angleIndex]));
            phity += readings[rIndex] * Math.sin(Utils.theta(lp.theta + laserAngles[angleIndex]));
//            IntPoint iphit = map.world2map(phitx, phity);
            int iphitx = (int) Math.round((phitx - map.getCenter().x) / map.getDelta()) + map.getSizeX2();
            int iphity = (int) Math.round((phity - map.getCenter().y) / map.getDelta()) + map.getSizeY2();
//            DoublePoint pfree = new DoublePoint(lp.x, lp.y);
            double pfreex = lp.x;
            double pfreey = lp.y;
            pfreex += (readings[rIndex] - freeDelta) * Math.cos(Utils.theta(lp.theta + laserAngles[angleIndex]));
            pfreey += (readings[rIndex] - freeDelta) * Math.sin(Utils.theta(lp.theta + laserAngles[angleIndex]));
            pfreex = pfreex - phitx;
            pfreey = pfreey - phity;

//            IntPoint ipfree = map.world2map(pfreex, pfreey);
            int ipfreex = (int) Math.round((pfreex - map.getCenter().x) / map.getDelta()) + map.getSizeX2();
            int ipfreey = (int) Math.round((pfreey - map.getCenter().y) / map.getDelta()) + map.getSizeY2();
            boolean found = false;
//            DoublePoint bestMu = new DoublePoint(0.0, 0.0);
            double bestMux = 0.0;
            double bestMuy = 0.0;
            for (int xx = -kernelSize; xx <= kernelSize; xx++) {
                for (int yy = -kernelSize; yy <= kernelSize; yy++) {
//                    IntPoint pr = new IntPoint(iphit.x + xx, iphit.y + yy);
                    int prx = iphitx + xx;
                    int pry = iphity + yy;

//                    IntPoint pf = new IntPoint(pr.x + ipfree.x, pr.y + ipfree.y);
                    int pfx = prx + ipfreex;
                    int pfy = pry + ipfreey;
                    //AccessibilityState s=map.storage().cellState(pr);
                    //if (s&Inside && s&Allocated){
                    PointAccumulator cell = (PointAccumulator) map.cell(prx, pry, true);
                    PointAccumulator fcell = (PointAccumulator) map.cell(pfx, pfy, true);
                    if (cell == null) {
                        cell = emptyPointAccumulator;
                    }
                    if (fcell == null) {
                        fcell = emptyPointAccumulator;
                    }
                    if (cell.doubleValue() > fullnessThreshold && fcell.doubleValue() < fullnessThreshold) {
                        double meanx = cell.accx / cell.n;
                        double meany = cell.accy / cell.n;
                        double mux = phitx - meanx;
                        double muy = phity - meany;
//                        DoublePoint mu = DoublePoint.minus(phit, cell.mean());
                        if (!found) {
                            bestMux = mux;
                            bestMuy = muy;
                            found = true;
                        } else {
                            double mulMu = mux * mux + muy * muy;
                            double mulBestMu = bestMux * bestMux + bestMuy * bestMuy;
                            if (mulMu < mulBestMu) {
                                bestMux = mux;
                                bestMuy = muy;
                            }
                        }
                    }
                }
            }
            double mulBestMu = bestMux * bestMux + bestMuy * bestMuy;
            if (found) {
                s += Math.exp(-1.0 / gaussianSigma * mulBestMu);
                c++;
            }
            double f = (1./ likelihoodSigma) * (mulBestMu);
            l += (found) ? f : noHit;
        }
        return new LikeliHoodAndScore(s, l, c);
    }

    public double icpStep(DoubleOrientedPoint pret, IGMap map, DoubleOrientedPoint p, double[] readings) {
        int angleIndex = initialBeamsSkip;
        DoubleOrientedPoint lp = new DoubleOrientedPoint(p.x, p.y, p.theta);

        lp.x += Math.cos(p.theta) * laserPose.x - Math.sin(p.theta) * laserPose.y;
        lp.y += Math.sin(p.theta) * laserPose.x + Math.cos(p.theta) * laserPose.y;
        lp.theta += laserPose.theta;
        int skip = 0;
        double freeDelta = map.getDelta() * freeCellRatio;
        List<DoublePointPair> pairs = new ArrayList<DoublePointPair>();

        for (int rIndex = initialBeamsSkip; rIndex < readings.length; rIndex++, angleIndex++) {
            skip++;
            skip = skip > likelihoodSkip ? 0 : skip;
            if (readings[rIndex] > usableRange || readings[rIndex] == 0.0) continue;
            if (skip != 0) continue;
            DoublePoint phit = new DoublePoint(lp.x, lp.y);

            phit.x += readings[rIndex] * Math.cos(lp.theta + laserAngles[angleIndex]);
            phit.y += readings[rIndex] * Math.sin(lp.theta + laserAngles[angleIndex]);
            IntPoint iphit = map.world2map(phit);

            DoublePoint pfree = new DoublePoint(lp.x, lp.y);
            pfree.x += (readings[rIndex] - map.getDelta() * freeDelta) * Math.cos(lp.theta + laserAngles[angleIndex]);
            pfree.y += (readings[rIndex] - map.getDelta() * freeDelta) * Math.sin(lp.theta + laserAngles[angleIndex]);
            pfree.x = pfree.x - phit.x;
            pfree.y = pfree.y - phit.y;

            IntPoint ipfree = map.world2map(pfree);
            boolean found = false;
            DoublePoint bestMu = new DoublePoint(0., 0.);
            DoublePoint bestCell = new DoublePoint(0., 0.);
            for (int xx = -kernelSize; xx <= kernelSize; xx++)
                for (int yy = -kernelSize; yy <= kernelSize; yy++) {
                    IntPoint pr = new IntPoint(iphit.x + xx, iphit.y + yy);
                    IntPoint pf = new IntPoint(pr.x + ipfree.x, pr.y + ipfree.y);
                    PointAccumulator cell = (PointAccumulator) map.cell(pr, true);
                    PointAccumulator fcell = (PointAccumulator) map.cell(pf, true);

                    if (cell.doubleValue() > fullnessThreshold && fcell.doubleValue() < fullnessThreshold) {
                        DoublePoint mu = DoublePoint.minus(phit, cell.mean());
                        if (!found) {
                            bestMu = mu;
                            bestCell = cell.mean();
                            found = true;
                        } else if (DoublePoint.mulD(mu, mu) < DoublePoint.mulD(bestMu, bestMu)) {
                            bestMu = mu;
                            bestCell = cell.mean();
                        }
                    }
                }
            if (found) {
                pairs.add(new DoublePointPair(phit, bestCell));
            }
        }

        DoubleOrientedPoint result = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        LOG.error("result(" + pairs.size() + ")=" + result.x + " " + result.y + " " + result.theta);
        pret.x = p.x + result.x;
        pret.y = p.y + result.y;
        pret.theta = p.theta + result.theta;
        pret.theta = Math.atan2(Math.sin(pret.theta), Math.cos(pret.theta));
        return score(map, p, readings);
    }

    double score(IGMap map, DoubleOrientedPoint p, double[] readings) {
        double s = 0;
        int angleIndex = initialBeamsSkip;
        DoubleOrientedPoint lp = new DoubleOrientedPoint(p.x, p.y, p.theta);
        lp.x += Math.cos(p.theta) * laserPose.x - Math.sin(p.theta) * laserPose.y;
        lp.y += Math.sin(p.theta) * laserPose.x + Math.cos(p.theta) * laserPose.y;
        lp.theta += laserPose.theta;
        int skip = 0;
        double freeDelta = map.getDelta() * freeCellRatio;
        for (int rIndex = initialBeamsSkip; rIndex < readings.length; rIndex++, angleIndex++) {
            skip++;
            skip = skip > likelihoodSkip ? 0 : skip;
            if (skip != 0 || readings[rIndex] > usableRange || readings[rIndex] == 0.0) continue;
            DoublePoint phit = new DoublePoint(lp.x, lp.y);
            phit.x += readings[rIndex] * Math.cos(Utils.theta(lp.theta + laserAngles[angleIndex]));
            phit.y += readings[rIndex] * Math.sin(Utils.theta(lp.theta + laserAngles[angleIndex]));
            IntPoint iphit = map.world2map(phit);
            DoublePoint pfree = new DoublePoint(lp.x, lp.y);
            pfree.x += (readings[rIndex] - map.getDelta() * freeDelta) * Math.cos(Utils.theta(lp.theta + laserAngles[angleIndex]));
            pfree.y += (readings[rIndex] - map.getDelta() * freeDelta) * Math.sin(Utils.theta(lp.theta + laserAngles[angleIndex]));
            pfree.x = pfree.x - phit.x;
            pfree.y = pfree.y - phit.y;

            IntPoint ipfree = map.world2map(pfree);
            boolean found = false;
            DoublePoint bestMu = new DoublePoint(0., 0.);
            for (int xx = -kernelSize; xx <= kernelSize; xx++) {
                for (int yy = -kernelSize; yy <= kernelSize; yy++) {
                    IntPoint pr = new IntPoint(iphit.x + xx, iphit.y + yy);
                    IntPoint pf = new IntPoint(pr.x + ipfree.x, pr.y + ipfree.y);
                    // int ss = map.getStorage().cellState(pr);
//                    if ((ss) > 0) {
                    PointAccumulator cell = (PointAccumulator) map.cell(pr, true);
                    PointAccumulator fcell = (PointAccumulator) map.cell(pf, true);
                    if (cell != null && fcell != null) {
                        if (cell.doubleValue() > fullnessThreshold && fcell.doubleValue() < fullnessThreshold) {
                            DoublePoint mu = DoublePoint.minus(phit, cell.mean());
                            if (!found) {
                                bestMu = mu;
                                found = true;
                            } else {
                                bestMu = DoublePoint.mulD(mu, mu) < DoublePoint.mulD(bestMu, bestMu) ? mu : bestMu;
                            }
                        }
                    }
//                    }
                }
            }
            if (found) {
                s += Math.exp(-1. / (gaussianSigma * DoublePoint.mulD(bestMu, bestMu)));
            }
        }
        return s;
    }

    private class ScoredMove {
        DoubleOrientedPoint pose;
        double score;
        double likelihood;

        private ScoredMove() {
        }

        private ScoredMove(DoubleOrientedPoint pose, double score, double likelihood) {
            this.pose = pose;
            this.score = score;
            this.likelihood = likelihood;
        }
    }
}
