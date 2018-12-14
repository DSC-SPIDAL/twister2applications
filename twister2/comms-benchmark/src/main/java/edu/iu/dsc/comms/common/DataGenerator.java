package edu.iu.dsc.comms.common;

import java.util.Random;

public final class DataGenerator {
    private DataGenerator() {
    }

    public static int[] generateIntData(int size) {
        int[] d = new int[size];
        for (int i = 0; i < size; i++) {
            d[i] = i;
        }
        return d;
    }

    public static int[] generateIntEmpty(int size) {
        int[] d = new int[size];
        for (int i = 0; i < size; i++) {
            d[i] = 0;
        }
        return d;
    }

    public static byte[] generateByteData(int size) {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return b;
    }

    public static int[] generateIntData(int size, int value) {
        int[] b = new int[size];
        for (int i = 0; i < size; i++) {
            b[i] = value;
        }
        return b;
    }

    public static double[] generateDoubleData(int size) {
        double[] b = new double[size];
        for (int i = 0; i < size; i++) {
            b[i] = 1;
        }
        return b;
    }
}
