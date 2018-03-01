package edu.iu.dsc.tws.storm;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SingleDataCollectionBolt extends BaseRichBolt {
  private static Logger LOG = LoggerFactory.getLogger(SingleDataCollectionBolt.class);
  private OutputCollector outputCollector;
  private int count = 0;
  private boolean debug = false;
  private int printInveral = 0;
  private TopologyContext context;

  @Override
  public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
    this.printInveral = (int) stormConf.get(Constants.ARGS_PRINT_INTERVAL);

    this.outputCollector = outputCollector;
    this.context = topologyContext;
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      outputCollector.ack(tuple);

      Long time = tuple.getLongByField(Constants.Fields.TIME_FIELD);
      if (debug && count % printInveral == 0) {
        LOG.info(context.getThisTaskId() + " Last Received tuple: " + count);
      }

      Object body = tuple.getValueByField(Constants.Fields.BODY);
      if (debug && count / printInveral == 0) {
        LOG.info("Size of the message: " + ((byte [])body).length);
      }

      count++;
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//    outputFieldsDeclarer.declareStream(Constants.Fields.CHAIN_STREAM, new Fields(
//        Constants.Fields.BODY,
//        Constants.Fields.MESSAGE_INDEX_FIELD,
//        Constants.Fields.MESSAGE_SIZE_FIELD,
//        Constants.Fields.TIME_FIELD,
//        Constants.Fields.TIME_FIELD2));
  }
}
