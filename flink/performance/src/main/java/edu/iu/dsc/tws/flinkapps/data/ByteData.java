package edu.iu.dsc.tws.flinkapps.data;

import java.io.Serializable;

public class ByteData implements Serializable {
  private byte[] data;

  public ByteData(int size) {
    data = new byte[size];
  }

  public ByteData() {
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }
}
