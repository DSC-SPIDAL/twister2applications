package edu.iu.dsc.tws.flinkapps.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CollectiveData {
  private int[] list;

  private Random random;

  public CollectiveData(int size) {
    random = new Random();
    list = new int[size];
    for (int i = 0; i < size; i++) {
      list[i] = (random.nextInt());
    }
  }

  public CollectiveData() {
  }

  public CollectiveData(int[] list) {
    this.list = list;
  }

  public int[] getList() {
    return list;
  }

  public void setList(int[] list) {
    this.list = list;
  }

  @Override
  public String toString() {
    return "CollectiveData{" +
        "list=" + list +
        '}';
  }
}
