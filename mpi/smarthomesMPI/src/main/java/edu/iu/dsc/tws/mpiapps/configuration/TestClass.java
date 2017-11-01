package edu.iu.dsc.tws.mpiapps.configuration;

import edu.iu.dsc.tws.mpiapps.MedianHeap;

/**
 * Created by pulasthi on 11/1/17.
 */
public class TestClass {
    public static void main(String[] args) {
        MedianHeap x = new MedianHeap();
        x.insertElement(3);
        x.insertElement(12);
        x.insertElement(9);
        x.insertElement(7);
        System.out.println(x.getMedian());
        x.insertElement(9);
        System.out.println(x.getMedian());


    }
}
