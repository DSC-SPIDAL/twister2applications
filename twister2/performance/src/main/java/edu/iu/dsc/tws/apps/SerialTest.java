package edu.iu.dsc.tws.apps;

import edu.iu.dsc.tws.apps.data.PartitionData;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

import java.util.HashMap;

public class SerialTest {
  private int iterations;

  private int size;

  private KryoSerializer serializer;

  public SerialTest(int iterations, int size) {
    this.iterations = iterations;
    this.size = size;
    this.serializer = new KryoSerializer();
    this.serializer.init(new HashMap<>());
  }

  public void run() {
    byte[] data = new byte[size];
    for (int i = 0; i < iterations; i++) {
      PartitionData p = new PartitionData(data, System.nanoTime(), 0);
      byte[]b = serializer.serialize(p);
    }
  }

  public static void main(String[] args) {
    int size = Integer.parseInt(args[1]);
    int iterations = Integer.parseInt(args[0]);

    long start = System.nanoTime();
    SerialTest test = new SerialTest(iterations, size);
    test.run();
    System.out.println("Total: " + (System.nanoTime() - start));
  }
}
