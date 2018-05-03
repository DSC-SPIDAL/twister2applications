package edu.iu.dsc.tws.apps.slam.core.gridfastsalm;

import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.Stat;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MotionModel {
    private static Logger LOG = LoggerFactory.getLogger(MotionModel.class);

    public double srr, str, srt, stt;

    public void setSrr(double srr) {
        this.srr = srr;
    }

    public void setStr(double str) {
        this.str = str;
    }

    public void setSrt(double srt) {
        this.srt = srt;
    }

    public void setStt(double stt) {
        this.stt = stt;
    }



    public DoubleOrientedPoint drawFromMotion(DoubleOrientedPoint p, double linearMove, double angularMove) {
        DoubleOrientedPoint n = new DoubleOrientedPoint(p);
        double lm = linearMove + Math.abs(linearMove) * Stat.sampleGaussian(srr, 0) + Math.abs(angularMove) * Stat.sampleGaussian(str, 0);
        double am = angularMove + Math.abs(linearMove) * Stat.sampleGaussian(srt, 0) + Math.abs(angularMove) * Stat.sampleGaussian(stt, 0);
        n.x += lm * Math.cos(n.theta + .5 * am);
        n.y += lm * Math.sin(n.theta + .5 * am);
        n.theta += am;
        n.theta = Math.atan2(Math.sin(n.theta), Math.cos(n.theta));
        return n;
    }

    public DoubleOrientedPoint drawFromMotionN(DoubleOrientedPoint p, DoubleOrientedPoint pnew, DoubleOrientedPoint pold) {
        return pnew;
    }

    public DoubleOrientedPoint drawFromMotion(DoubleOrientedPoint p, DoubleOrientedPoint pnew, DoubleOrientedPoint pold) {
        double sxy = 0.3 * srr;
        DoubleOrientedPoint delta = absoluteDifference(pnew, pold);
//        DoubleOrientedPoint delta = DoubleOrientedPoint.minus(pnew, pold);
        DoubleOrientedPoint noisypoint = new DoubleOrientedPoint(delta);
        noisypoint.x += Stat.sampleGaussian(srr * Math.abs(delta.x) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.y), 0);
        noisypoint.y += Stat.sampleGaussian(srr * Math.abs(delta.y) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.x), 0);

//        noisypoint.x += Stat.sampleGaussian(srr * Math.abs(delta.x) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.y), 0);
//        noisypoint.y += Stat.sampleGaussian(srr * Math.abs(delta.y) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.x), 0);

        noisypoint.theta += Stat.sampleGaussian(stt * Math.abs(delta.theta) + srt * Math.sqrt(delta.x * delta.x + delta.y * delta.y), 0);
        noisypoint.theta = noisypoint.theta % (2 * Math.PI);
        if (noisypoint.theta > Math.PI)
            noisypoint.theta -= 2 * Math.PI;
        DoubleOrientedPoint point = absoluteSum(p, noisypoint);

//        LOG.info("*************** old pos {} new pos {} changed pos {} ************ ", pold, pnew, point);

        return point;
    }

    public DoubleOrientedPoint drawFromMotionRandomL(DoubleOrientedPoint p, DoubleOrientedPoint pnew, DoubleOrientedPoint pold) {
        double sxy = 0.3 * srr;

        DoubleOrientedPoint delta = absoluteDifference(pnew, pold);
        DoubleOrientedPoint noisypoint = new DoubleOrientedPoint(delta);
        NormalDistribution normalDistribution = new NormalDistribution(srr * Math.abs(delta.x) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.y), .005);
        noisypoint.x += normalDistribution.sample();
        normalDistribution = new NormalDistribution(srr * Math.abs(delta.y) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.x), .005);
        noisypoint.y += normalDistribution.sample();
        normalDistribution = new NormalDistribution(stt * Math.abs(delta.theta) + srt * Math.sqrt(delta.x * delta.x + delta.y * delta.y), .005);
        noisypoint.theta += normalDistribution.sample();
//        noisypoint.x += Stat.sampleGaussian(srr * Math.abs(delta.x) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.y), 0);
//        noisypoint.y += Stat.sampleGaussian(srr * Math.abs(delta.y) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.x), 0);

//        noisypoint.x += Stat.sampleGaussian(srr * Math.abs(delta.x) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.y), 0);
//        noisypoint.y += Stat.sampleGaussian(srr * Math.abs(delta.y) + str * Math.abs(delta.theta) + sxy * Math.abs(delta.x), 0);

//        noisypoint.theta += Stat.sampleGaussian(stt * Math.abs(delta.theta) + srt * Math.sqrt(delta.x * delta.x + delta.y * delta.y), 0);
        noisypoint.theta = noisypoint.theta % (2 * Math.PI);
        if (noisypoint.theta > Math.PI)
            noisypoint.theta -= 2 * Math.PI;
        DoubleOrientedPoint point = absoluteSum(p, noisypoint);

//        LOG.info("*************** old pos {} new pos {} changed pos {} ************ ", pold, pnew, point);

        return point;
    }

    public static DoubleOrientedPoint absoluteSum(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double s = Math.sin(p1.theta);
        double c = Math.cos(p1.theta);
        double x = c * p2.x - s * p2.y + p1.x;
        double y = s * p2.x + c * p2.y + p1.y;
//        double theta = p2.theta + p1.theta;
        double theta = Math.atan2(Math.sin(p2.theta + p1.theta), Math.cos(p2.theta + p1.theta));
        return new DoubleOrientedPoint(x, y, theta);
    }

    public static DoubleOrientedPoint absoluteDifference(DoubleOrientedPoint p1, DoubleOrientedPoint p2) {
        double x = p1.x - p2.x;
        double y = p1.y - p2.y;
        double theta = p1.theta - p2.theta;

        theta = Math.atan2(Math.sin(theta), Math.cos(theta));
        double s = Math.sin(p2.theta), c = Math.cos(p2.theta);
        // TODO: check the s * y
        return new DoubleOrientedPoint(c * x + s * y, -s * x + c * y, theta);
    }
}
