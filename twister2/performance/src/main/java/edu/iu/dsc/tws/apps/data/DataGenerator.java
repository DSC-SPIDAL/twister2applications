package edu.iu.dsc.tws.apps.data;

import edu.iu.dsc.tws.apps.common.RandomString;
import edu.iu.dsc.tws.apps.utils.JobParameters;
import edu.iu.dsc.tws.comms.mpi.io.IntData;

import java.util.Random;

public class DataGenerator {
  JobParameters jobParameters;

  RandomString randomString;

  public DataGenerator(JobParameters jobParameters) {
    this.jobParameters = jobParameters;
    randomString = new RandomString(jobParameters.getSize(), new Random(), RandomString.alphanum);
  }
  /**
   * Generate datacols with an integer array
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

  public byte[] generateByteData() {
    int s = jobParameters.getSize();
    byte b[] = new byte[s];
    new Random().nextBytes(b);
    return b;
  }

  public String generateStringData() {
    return randomString.nextString();
  }
}
