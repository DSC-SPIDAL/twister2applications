package edu.iu.dsc.tws.flinkapps.svm;

import java.io.Serializable;
import java.util.Random;

public class PegasosSGD implements Serializable {

    private double[] xyia;
    private double[] wa;
    private final double alpha = 0.001;
    private double[] newW;

    public PegasosSGD() {
        this.newW = new double[22];
        Random random = new Random();
        for (int i = 0; i < 22; i++) {
            this.newW[i] = random.nextGaussian();
        }
    }

    public double[] onlineSGD(double[] w, double[] x, double y) {
        double condition = y * Matrix.dot(x, w);
        double[] newW;
        if (condition < 1) {
            this.xyia = new double[x.length];
            this.xyia = Matrix.scalarMultiply(Matrix.subtract(w, Matrix.scalarMultiply(x, y)), alpha);
            this.newW = Matrix.subtract(w, xyia);
        } else {
            wa = new double[x.length];
            wa = Matrix.scalarMultiply(w, alpha);
            this.newW = Matrix.subtract(w, wa);
        }
        return this.newW;
    }

    public double[] getNewW() {
        return newW;
    }
}
