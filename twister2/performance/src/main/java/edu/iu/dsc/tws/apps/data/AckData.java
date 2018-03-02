package edu.iu.dsc.tws.apps.data;

public class AckData {
  private long time;

  private long id;

  public AckData(long time, long id) {
    this.time = time;
    this.id = id;
  }

  public AckData() {
  }

  public long getTime() {
    return time;
  }

  public long getId() {
    return id;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setId(long id) {
    this.id = id;
  }
}
