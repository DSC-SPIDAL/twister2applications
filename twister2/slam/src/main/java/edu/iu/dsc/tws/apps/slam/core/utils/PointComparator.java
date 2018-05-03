package edu.iu.dsc.tws.apps.slam.core.utils;

import java.util.Comparator;

public class PointComparator implements Comparator<IntPoint> {
    @Override
    public int compare(IntPoint o1, IntPoint o2) {

        //boolean equal = (Integer)o1.x < (Integer)o2.x || (o1.x.equals(o2.x) && (Integer)o1.y < (Integer)o2.y);
        boolean equal = ((Integer) o1.x).equals((Integer) o2.x) && ((Integer) o1.y).equals((Integer) o2.y);
        if (equal) {
            return 0;
        } else {
            return 1;
        }

    }
}
