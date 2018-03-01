package edu.iu.dsc.tws.apps.storm;

public class Worker implements Runnable {
  private PartitionSource source;

  public Worker(PartitionSource source) {
    this.source = source;
  }

  @Override
  public void run() {
    source.execute();
  }
}
