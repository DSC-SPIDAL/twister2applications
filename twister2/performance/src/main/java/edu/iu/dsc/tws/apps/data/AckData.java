package edu.iu.dsc.tws.apps.data;

public class AckData {
  private long time;

  private long id;

  private Object data;

  public AckData(long time, long id) {
    this.time = time;
    this.id = id;
  }

  public AckData() {
  }

  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
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
