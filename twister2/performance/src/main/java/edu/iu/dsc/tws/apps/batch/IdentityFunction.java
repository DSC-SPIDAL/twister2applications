package edu.iu.dsc.tws.apps.batch;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class IdentityFunction implements ReduceFunction {
  private static final Logger LOG = Logger.getLogger(IdentityFunction.class.getName());

  private int count = 0;
  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
  }

  @Override
  public Object reduce(Object t1, Object t2) {
    count++;
    if (count % 100 == 0) {
      LOG.info(String.format("Partial received %d", count));
    }
    return t1;
  }
}