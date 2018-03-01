package edu.iu.dsc.tws.storm;

public class Constants {
  public static final String ARGS_NAME = "name";
  public static final String ARGS_LOCAL = "local";
  public static final String ARGS_PARALLEL = "p";
  public static final String ARGS_SREAM_MGRS = "stmgr";
  public static final String ARGS_DEBUG = "g";
  public static final String ARGS_PRINT_INTERVAL = "pi";
  public static final String UPPER_COMPONENT_NAME = "upperComponentName";

  public static final String ARGS_THRPUT_SIZES = "thrSizes";
  public static final String ARGS_THRPUT_NO_MSGS = "thrN";
  public static final String ARGS_THRPUT_NO_EMPTY_MSGS = "thrNEmpty";
  public static final String ARGS_MODE = "mode";
  public static final String ARGS_RATE = "rate";
  public static final String ARGS_URL = "url";
  public static final String ARGS_THRPUT_FILENAME = "thrF";
  public static final String ARGS_SEND_INTERVAL = "int";

  // configurations
  public static final String ARGS_SPOUT_PARALLEL = "sp";
  public static final String ARGS_MAX_PENDING = "mp";

  public abstract class Fields {
    public static final String BODY = "body";
    public static final String CHAIN_STREAM = "chain";
    public static final String CONTROL_STREAM = "control";
    public static final String TIME_FIELD = "time";
    public static final String TIME_FIELD2 = "time2";
    public static final String SENSOR_ID_FIELD = "sensorID";
    public static final String MESSAGE_SIZE_FIELD = "messageSize";
    public static final String MESSAGE_INDEX_FIELD = "messageIndex";
  }

  public abstract class Topology {
    public static final String SPOUT = "spout";
    public static final String LAST = "lastbolt";
    public static final String DATAGEN = "datagenbolt";
    public static final String SEND = "sendbolt";
    public static final String ORIGIN = "originbolt";
    public static final String MULTI_DATAGATHER = "multidatagather";
    public static final String SINGLE_DATAGATHER = "singledatagather";
  }
}
