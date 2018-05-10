package edu.iu.dsc.tws.apps.slam.streaming.ops;

import edu.iu.dsc.tws.comms.api.MessageType;

public class OpRequest {
  private Object data;

  private int task;

  private MessageType type;

  public OpRequest(Object data, int task, MessageType type) {
    this.data = data;
    this.task = task;
    this.type = type;
  }

  public Object getData() {
    return data;
  }

  public int getTask() {
    return task;
  }

  public MessageType getType() {
    return type;
  }
}
