package edu.iu.dsc.tws.flinkapps.data;

import java.io.Serializable;
import java.util.Random;

public class CollectiveDoubleData implements Serializable {

    private double[] list;

    private Random random;

    public CollectiveDoubleData(int size) {
        random = new Random();
        list = new double[size];
        for (int i = 0; i < size; i++) {
            list[i] = (random.nextInt());
        }
    }

    public CollectiveDoubleData() {
    }

    public CollectiveDoubleData(double[] list) {
        this.list = list;
    }

    public double[] getList() {
        return list;
    }

    public void setList(double[] list) {
        this.list = list;
    }

    @Override
    public String toString() {
        return "CollectiveDoubleData{" +
                "list=" + list +
                '}';
    }
}
