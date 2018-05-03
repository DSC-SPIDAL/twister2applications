package edu.iu.dsc.tws.apps.slam.streaming;

import java.util.HashMap;
import java.util.Map;

public class Tuple {
  private Map<String, Object> values = new HashMap<>();

  private String sourceStreamId;

  public Tuple(Map<String, Object> values, String sourceStreamId) {
    this.values = values;
    this.sourceStreamId = sourceStreamId;
  }

  public Object getValueByField(String field) {
    return values.get(field);
  }

  public String getSourceStreamId() {
    return sourceStreamId;
  }
}
