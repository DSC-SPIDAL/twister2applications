package edu.iu.dsc.tws.flinkapps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CollectiveData implements Serializable {
  private List<Integer> list = new ArrayList<>();

  public CollectiveData(int size, int value) {
    for (int i = 0; i < size; i++) {
      list.add(value);
    }
  }

  public CollectiveData(List<Integer> list) {
    this.list = list;
  }

  public CollectiveData() {
  }

  public List<Integer> getList() {
    return list;
  }

  public void setList(List<Integer> list) {
    this.list = list;
  }

  @Override
  public String toString() {
    return "CollectiveData{" +
        "list=" + list +
        '}';
  }
}
