package edu.iu.dsc.tws.mpiapps.data;

public class IntData {
  private int[] data;

  public IntData(int[] data) {
    this.data = data;
  }

  public IntData() {
  }

  public int[] getData() {
    return data;
  }

  public void setData(int[] data) {
    this.data = data;
  }
}
