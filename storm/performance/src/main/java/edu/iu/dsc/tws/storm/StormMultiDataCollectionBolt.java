package edu.iu.dsc.tws.storm;

import com.esotericsoftware.kryo.Kryo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StormMultiDataCollectionBolt extends BaseRichBolt {

    private static Logger LOG = Logger.getLogger(StormMultiDataCollectionBolt.class.getName());

    private OutputCollector outputCollector;
    private int count = 0;
    private boolean debug = false;
    private long printInveral = 0;
    private TopologyContext context;
    private Map<Integer, Queue<Tuple>> incoming = new HashMap<>();
    private Map<Integer, Queue<Long>> arrivalTimes = new HashMap<>();
    private Kryo kryo;
    private boolean passThrough = false;
    private String upperComponentName;
    private boolean gather = false;

    public boolean isPassThrough() {
        return passThrough;
    }

    public void setPassThrough(boolean passThrough) {
        this.passThrough = passThrough;
    }

    public String getUpperComponentName() {
        return upperComponentName;
    }

    public void setUpperComponentName(String upperComponentName) {
        this.upperComponentName = upperComponentName;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        String upName = (String) stormConf.get(Constants.UPPER_COMPONENT_NAME);
        if (upName != null) {
            upperComponentName = upName;
        }
        List<Integer> taskIds = topologyContext.getComponentTasks(upperComponentName);
        this.debug = (boolean) stormConf.get(Constants.ARGS_DEBUG);
        this.printInveral = (long) stormConf.get(Constants.ARGS_PRINT_INTERVAL);

        this.outputCollector = outputCollector;
        this.context = topologyContext;
        this.kryo = new Kryo();

        for (int t : taskIds) {
            incoming.put(t, new LinkedList<Tuple>());
            arrivalTimes.put(t, new LinkedList<Long>());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        int sourceTask = tuple.getSourceTask();
        Queue<Tuple> queue = incoming.get(sourceTask);
        queue.add(tuple);
        Queue<Long> arrivalQueue = arrivalTimes.get(sourceTask);
        arrivalQueue.add(System.nanoTime());

        boolean allIn = true;
        for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
            if (e.getValue().size() <= 0) {
                allIn = false;
            }
        }

        if (allIn) {
            List<Long> timings = new ArrayList<>();
            List<Long> arrivalTimesList = new ArrayList<>();
            List<Tuple> anchors = new ArrayList<>();
            Object body = null;
            int index = -1;
            int tmpIndex;
            for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
                Tuple t = e.getValue().poll();
                tmpIndex = t.getIntegerByField(Constants.Fields.MESSAGE_INDEX_FIELD);
                if (index == -1) {
                    index = tmpIndex;
                } else {
                    if (tmpIndex != index) {
                        LOG.severe(String.format("Indexes are not equal, something is wrong %d %d", index, tmpIndex));
                    }
                }
                body = tuple.getValueByField(Constants.Fields.BODY);
                anchors.add(t);
                arrivalTimesList.add(arrivalTimes.get(e.getKey()).poll());
                timings.add(t.getLongByField(Constants.Fields.TIME_FIELD));
                byte []b = (byte[]) body;
                if (debug && count % printInveral == 0) {
                    LOG.info("Size of the message: " + b.length);
                }
            }

            for (Tuple t : anchors) {
                outputCollector.ack(t);
            }
        }

        if (debug && count % printInveral == 0) {
            for (Map.Entry<Integer, Queue<Tuple>> e : incoming.entrySet()) {
                LOG.log(Level.INFO, String.format("%d Incoming %d, %d total: %d",
                        context.getThisTaskId(), e.getKey(), e.getValue().size(), 0));
            }
            LOG.info(context.getThisTaskId() + " Last Received tuple: " + count);
        }
        count++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
