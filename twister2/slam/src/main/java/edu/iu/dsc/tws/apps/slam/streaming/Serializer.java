package edu.iu.dsc.tws.apps.slam.streaming;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.InputStream;
import java.util.Map;

public class Serializer {
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  public Serializer(Kryo kryo) {
    this.kryo = kryo;
    kryo.setReferences(false);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  public Serializer() {
    kryo = new Kryo();
    kryo.setReferences(false);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  public void init(Map<String, Object> config) {
  }

  public byte[] serialize(Object object) {
    kryoOut.clear();
    kryo.writeClassAndObject(kryoOut, object);
    return kryoOut.toBytes();
  }

  public Object deserialize(byte[] input) {
    kryoIn.setBuffer(input);
    return kryo.readClassAndObject(kryoIn);
  }

  public Object deserialize(InputStream inputStream) {
    Input input = new Input(inputStream);
    return kryo.readClassAndObject(input);
  }
}
