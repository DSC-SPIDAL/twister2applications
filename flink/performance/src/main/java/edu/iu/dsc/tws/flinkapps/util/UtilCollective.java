package edu.iu.dsc.tws.flinkapps.util;

import edu.iu.dsc.tws.flinkapps.data.CollectiveData;

public class UtilCollective {

    public static CollectiveData add(CollectiveData i, CollectiveData j) {
        int[] r = new int[i.getList().length];
        for (int k = 0; k < i.getList().length; k++) {
            r[k] = ((i.getList()[k] + j.getList()[k]));
        }
        return new CollectiveData(r, i.getIteration() + ":" + j.getIteration());
    }
}
