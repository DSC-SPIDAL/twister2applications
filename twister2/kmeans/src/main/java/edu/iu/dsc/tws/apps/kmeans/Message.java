package edu.iu.dsc.tws.apps.kmeans;

public class Message {
  private int target;

  private int source;

  private Object message;

  public Message(int target, int source, Object message) {
    this.target = target;
    this.source = source;
    this.message = message;
  }

  public int getTarget() {
    return target;
  }

  public int getSource() {
    return source;
  }

  public Object getMessage() {
    return message;
  }
}
