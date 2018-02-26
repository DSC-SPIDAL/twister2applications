package edu.iu.dsc.tws.apps.data;

import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.mpi.io.IntData;

public class DataGenerator {
  JobParameters jobParameters;

  public DataGenerator(JobParameters jobParameters) {
    this.jobParameters = jobParameters;
  }
  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  public IntData generateData() {
    int s = jobParameters.getSize();
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public int[] generateIntData() {
    int s = jobParameters.getSize();
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return d;
  }
}
