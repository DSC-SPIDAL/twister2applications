package edu.iu.dsc.tws.apps.slam.core.utils;

public class Covariance3 {
    public double xx, yy, tt, xy, xt, yt;

    public Covariance3() {
    }

    public Covariance3(double xx, double yy, double tt, double xy, double xt, double yt) {
        this.xx = xx;
        this.yy = yy;
        this.tt = tt;
        this.xy = xy;
        this.xt = xt;
        this.yt = yt;
    }
}
