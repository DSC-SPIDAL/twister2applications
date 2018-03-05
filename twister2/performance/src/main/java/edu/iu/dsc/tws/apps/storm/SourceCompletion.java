package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.comms.api.CompletionListener;

import java.util.List;

public class SourceCompletion implements CompletionListener {
  private List<PartitionSource> source;

  private int currentIndex = 0;

  public SourceCompletion(List<PartitionSource> source) {
    this.source = source;
  }

  @Override
  public void writeReady(int i, int i1) {
    PartitionSource s = source.get(currentIndex);
    if (s.execute()) {
      currentIndex++;
      currentIndex = currentIndex % source.size();
    }
  }

  @Override
  public void readReady(int i, int i1) {

  }
}
