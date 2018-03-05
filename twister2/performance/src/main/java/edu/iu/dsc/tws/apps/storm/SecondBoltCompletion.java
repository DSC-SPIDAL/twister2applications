package edu.iu.dsc.tws.apps.storm;

import edu.iu.dsc.tws.comms.api.CompletionListener;

import java.util.List;

public class SecondBoltCompletion implements CompletionListener {
  private List<SecondBolt> secondBolts;

  private int currentIndex;

  public SecondBoltCompletion() {
  }

  @Override
  public void writeReady(int i, int i1) {

  }

  @Override
  public void readReady(int i, int i1) {

  }
}
