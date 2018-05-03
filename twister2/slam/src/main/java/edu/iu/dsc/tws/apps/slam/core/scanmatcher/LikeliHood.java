package edu.iu.dsc.tws.apps.slam.core.scanmatcher;

import edu.iu.dsc.tws.apps.slam.core.utils.Covariance3;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;


public class LikeliHood {
    public double _lmax;
    public DoubleOrientedPoint _mean;
    public Covariance3 _cov;
    public double likeliHood;

    public LikeliHood(double _lmax, DoubleOrientedPoint _mean, Covariance3 _cov, double likeliHood) {
        this._lmax = _lmax;
        this._mean = _mean;
        this._cov = _cov;
        this.likeliHood = likeliHood;
    }
}
