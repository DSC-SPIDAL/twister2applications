package edu.iu.dsc.tws.apps.slam.streaming;

public class Message {
  private byte[] body;

  public Message(byte[] data) {
    this.body = data;
  }

  public byte[] getBody() {
    return body;
  }
}
