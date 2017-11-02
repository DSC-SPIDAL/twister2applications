package edu.iu.dsc.tws.mpiapps;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by pulasthi on 11/1/17.
 */
public class MedianHeap {
    PriorityQueue<Double> maxq;
    PriorityQueue<Double> minq;

    public MedianHeap(){
        maxq = new PriorityQueue<Double>(17, Collections.reverseOrder());
        minq = new PriorityQueue<Double>(17);
    }

    public void insertElement(double element){
        if(maxq.size() == 0){
            if(maxq.size() == minq.size()){
                maxq.add(element);
                return;
            }

            if(element > minq.peek()){
                minq.add(element);
                maxq.add(minq.poll());
            }else{
                maxq.add(element);
            }
            return;
        }

        if(element < maxq.peek()){
            maxq.add(element);
        }else{
            minq.add(element);
        }

        if(maxq.size() == minq.size() + 2){
            minq.add(maxq.poll());
        }else if(minq.size() == maxq.size() + 2){
            maxq.add(minq.poll());
        }
    }

    public double getMedian(){
        if(maxq.size() == 0 || minq.size() == 0){
            if(maxq.size() == minq.size()){
                return 0.0;
            }else if(maxq.size() == 0){
                return minq.peek();
            }else{
                return maxq.peek();
            }
        }
        if(maxq.size() > minq.size()){
            return maxq.peek();
        }else if(maxq.size() < minq.size()){
            return minq.peek();
        }else{
            return (minq.peek() + maxq.peek())/2;
        }
    }

}
