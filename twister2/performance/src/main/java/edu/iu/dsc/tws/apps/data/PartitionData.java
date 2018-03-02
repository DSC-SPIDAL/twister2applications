package edu.iu.dsc.tws.apps.data;

public class PartitionData {
  private byte data[];

  private long time;

  private long id;

  public PartitionData(byte[] data, long time, long id) {
    this.data = data;
    this.time = time;
    this.id = id;
  }

  public PartitionData() {
  }

  public byte[] getData() {
    return data;
  }

  public long getTime() {
    return time;
  }

  public long getId() {
    return id;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setId(long id) {
    this.id = id;
  }
}
