package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.streaming.msgs.Ready;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.Trace;
import com.esotericsoftware.kryo.Kryo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DispatcherBolt {
    private static Logger LOG = LoggerFactory.getLogger(DispatcherBolt.class);

    private Kryo kryo;
    private Kryo mainKryo;

    private Map conf;

    private int noOfParallelTasks = 0;

    private Tuple currentTuple;

    private Lock lock = new ReentrantLock();

    private enum State {
        WAITING_FOR_READING,
        WAITING_FOR_READY,
        WAITING_ANY
    }

    private State state = State.WAITING_FOR_READING;

    public void prepare(Map stormConf) {
        this.conf = stormConf;

        // todo: set this value
        this.noOfParallelTasks = 0;

        kryo = new Kryo();
        mainKryo = new Kryo();
        Utils.registerClasses(kryo);
        Utils.registerClasses(mainKryo);
    }

    private List<Ready> readyList = new ArrayList<Ready>();

    private long beginTime;

    private long previousTime;

    private long tempBeginTime;

    public void execute(Tuple input) {
        String stream = input.getSourceStreamId();
        if (stream.equals(Constants.Fields.READY_STREAM)) {
            byte readyBytes[] = (byte[]) input.getValueByField(Constants.Fields.READY_FIELD);
            Ready ready = (Ready) Utils.deSerialize(kryo, readyBytes, Ready.class);
            handleReady(ready);
            return;
        }

        if (stream.equals(Constants.Fields.CONTROL_STREAM)) {
            readyList.clear();
            this.state = State.WAITING_FOR_READING;
            return;
        }

        lock.lock();
        try {
            tempBeginTime = System.currentTimeMillis();
            if (this.state == State.WAITING_FOR_READING) {
                beginTime = tempBeginTime;
                Trace t = new Trace();
                t.setPd(previousTime);
                // todo
//                outputCollector.emit(Constants.Fields.SCAN_STREAM, createTuple(input, t));
                this.currentTuple = null;
                this.state = State.WAITING_ANY;
                LOG.info("Changing state from READING to ANY");
            } else if (this.state == State.WAITING_ANY) {
                this.currentTuple = input;
                this.state = State.WAITING_FOR_READY;
                LOG.info("Changing state from ANY to READY");
            } else if (this.state == State.WAITING_FOR_READY) {
                this.currentTuple = input;
                LOG.info("Input while in state READY");
            }
        } finally {
            lock.unlock();
        }
    }


    private void handleReady(Ready ready) {
        lock.lock();
        readyList.add(ready);
        try {
            if (readyList.size() == noOfParallelTasks) {
                previousTime = System.currentTimeMillis() - beginTime;
                if (state == State.WAITING_FOR_READY) {
                    readyList.clear();
                    currentTuple = null;
                    state = State.WAITING_FOR_READING;
                    LOG.info("Changing state from READY to ANY");
                } else if (state == State.WAITING_ANY) {
                    state = State.WAITING_FOR_READING;
                    readyList.clear();
                    LOG.info("Changing state from ANY to READING");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private List<Object> createTuple(Tuple currentTuple, Trace trace) {
        Object body = currentTuple.getValueByField(Constants.Fields.BODY);
        Object time = currentTuple.getValueByField(Constants.Fields.TIME_FIELD);
        Object sensorId = currentTuple.getValueByField(Constants.Fields.SENSOR_ID_FIELD);

        List<Object> emit = new ArrayList<Object>();
        emit.add(body);
        emit.add(sensorId);
        emit.add(time);

        byte []traceBytes = Utils.serialize(mainKryo, trace);
        emit.add(traceBytes);

        return emit;
    }
}
