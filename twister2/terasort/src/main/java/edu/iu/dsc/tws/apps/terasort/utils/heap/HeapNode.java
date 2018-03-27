package edu.iu.dsc.tws.apps.terasort.utils.heap;


import edu.iu.dsc.tws.apps.terasort.utils.Record;

public class HeapNode {
  public Record data;
  public int listNo;

  public HeapNode(Record data, int listNo) {
    this.data = data;
    this.listNo = listNo;
  }
}
