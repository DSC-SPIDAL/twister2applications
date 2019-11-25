package edu.iu.dsc.tws.flinkapps.svm;

import java.io.Serializable;
import java.util.logging.Logger;

public final class Matrix implements Serializable {

    private static final Logger LOG = Logger.getLogger(Matrix.class.getName());
    private static final long serialVersionUID = -5712263538644901408L;

    private Matrix() {
    }

    public static double[] scalarMultiply(double[] x, double y) {
        double[] result = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            result[i] = x[i] * y;
        }
        return result;
    }

    public static double sum(double[] arr) {
        double sum = -1;
        for (int i = 0; i < arr.length; i++) {
            sum += arr[i];
        }
        return sum;
    }

    public static double[] scalarMultiplyR(double[] x, double y, double[] result) {
        for (int i = 0; i < x.length; i++) {
            result[i] = x[i] * y;
        }
        return result;
    }

    public static double[] scalarDivide(double[] x, double y) {
        double[] result = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            result[i] = x[i] / y;
        }
        return result;
    }



    public static double[] scalarAddition(double[] x, double y) {
        double[] result = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            result[i] = x[i] + y;
        }
        return result;
    }

    public static double[] multiply(double[] x, double[] w) {
        if (x.length == w.length) {
            double[] result = new double[x.length];
            for (int i = 0; i < x.length; i++) {
                result[i] = x[i] * w[i];
            }
            return result;
        } else {
            return null;
        }
    }

    public static double[] divide(double[] x, double[] w) {
        if (x.length == w.length) {
            double[] result = new double[x.length];
            for (int i = 0; i < x.length; i++) {
                result[i] = x[i] / w[i];
            }
            return result;
        } else {
            return null;
        }
    }

    public static double[] sqrt(double[] x) {
        if (x.length > 0) {
            double[] result = new double[x.length];
            for (int i = 0; i < x.length; i++) {
                result[i] = Math.sqrt(x[i]);
            }
            return result;
        } else {
            return null;
        }
    }

    public static double[] add(double[] w1, double[] w2)  {
        if (w1.length == w2.length) {
            double[] result = new double[w1.length];
            for (int i = 0; i < w1.length; i++) {
                result[i] = w1[i] + w2[i];
            }
            return result;
        } else {
            return null;
        }

    }

    public static double[] subtract(double[] w1, double[] w2) {
        if (w1.length == w2.length) {
            double[] result = new double[w1.length];
            for (int i = 0; i < w1.length; i++) {
                result[i] = w1[i] - w2[i];
            }
            return result;
        } else {
            return null;
        }
    }

    public static double[] subtractR(double[] w1, double[] w2, double[] res)   {
        if (w1.length == w2.length) {
            for (int i = 0; i < w1.length; i++) {
                res[i] = w1[i] - w2[i];
            }
            return res;
        } else {
            return null;
        }
    }

    public static double dot(double[] x, double[] w)  {
        double result = 0;
        if (x == null) {
            throw new NullPointerException("X is null");
        }
        if (w == null) {
            throw new NullPointerException("w is null");
        }
        if (x != null && w != null) {
            if (x.length == w.length) {
                for (int i = 0; i < x.length; i++) {
                    result += x[i] * w[i];
                }
            }
        }
        return result;
    }


    public static void printVector(double[] mat) {
        String s = "";
        for (int i = 0; i < mat.length; i++) {
            s += mat[i] + " ";
        }
        LOG.info("Print Matrix : " + s);
    }

    public static void printMatrix(double[][] mat) {
        for (int i = 0; i < mat.length; i++) {
            for (int j = 0; j < mat[0].length; j++) {
                System.out.print(mat[i][j] + " ");
            }
            System.out.println();
        }
        System.out.println();
    }
}
