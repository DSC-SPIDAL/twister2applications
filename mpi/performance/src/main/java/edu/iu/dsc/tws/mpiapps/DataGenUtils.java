package edu.iu.dsc.tws.mpiapps;

import edu.iu.dsc.tws.mpiapps.data.IntData;

public final class DataGenUtils {
  private DataGenUtils() {
  }

  /**
   * Generate datacols with an integer array
   *
   * @return IntData
   */
  public static IntData generateData(int s) {
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public static int[] generateIntData(int size) {
    int[] d = new int[size];
    for (int i = 0; i < size; i++) {
      d[i] = i;
    }
    return d;
  }

}
