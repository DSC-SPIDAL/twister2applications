package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.Ready;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.Trace;
import edu.iu.dsc.tws.apps.slam.utils.FileIO;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowBroadcast;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import mpi.Intracomm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DispatcherTask {
  private static Logger LOG = LoggerFactory.getLogger(DispatcherTask.class);

  private Serializer kryo;
  private Serializer mainKryo;

  private Map conf;

  private int noOfParallelTasks = 0;

  private Lock lock = new ReentrantLock();

  private SlamDataReader dataReader;

  private KryoMemorySerializer kryoMemorySerializer;

  private int task;

  private boolean simbad = true;

  private enum State {
    WAITING_FOR_READING,
    WAITING_FOR_READY,
  }

  private State state = State.WAITING_FOR_READING;

  private long startTime = System.currentTimeMillis();

  private FileIO fileIo;

  public void prepare(Map stormConf, Intracomm intracomm, String inputFile, int task, boolean simbad) {
    this.simbad = simbad;
    this.conf = stormConf;
    this.task = task;

    // todo: set this value
    this.noOfParallelTasks = 0;

    kryo = new Serializer();
    mainKryo = new Serializer();
//    Utils.registerClasses(kryo);
//    Utils.registerClasses(mainKryo);
    if (!simbad) {
      this.dataReader = new SlamDataReader(inputFile);
    } else {
      this.fileIo = new FileIO(inputFile, false);
    }
  }

  private long beginTime;

  private long previousTime;

  private long tempBeginTime;

  private MPIDataFlowBroadcast broadcast;

  public void setBroadcast(MPIDataFlowBroadcast broadcast) {
    this.broadcast = broadcast;
  }

  public void execute(Tuple input) {
    lock.lock();
    try {
      tempBeginTime = System.currentTimeMillis();
      if (this.state == State.WAITING_FOR_READING) {
        beginTime = tempBeginTime;
        Trace t = new Trace();
        t.setPd(previousTime);
        // todo
        lastSendTime = System.currentTimeMillis();
        broadcast.send(task, input, 0);
        this.state = State.WAITING_FOR_READY;
        LOG.info("Changing state from READING to ANY");
      }
    }  finally {
      lock.unlock();
    }
  }

  public void handleReady(Ready ready) {
    lock.lock();
    try {
      state = State.WAITING_FOR_READING;
      LOG.info("Time: " + (System.currentTimeMillis() - lastSendTime) + " Total: " + (System.currentTimeMillis() - startTime * 1.0) / 1000.0 + " number: " + count);
    } finally {
      lock.unlock();
    }
  }

  private Tuple createTuple(LaserScan scan, Trace trace) {
    Map<String, Object> objects = new HashMap<>();
    objects.put(Constants.Fields.BODY, scan);
    objects.put(Constants.Fields.TIME_FIELD, System.currentTimeMillis());
    objects.put(Constants.Fields.SENSOR_ID_FIELD, "");

//    byte[] traceBytes = mainKryo.serialize(trace);
    objects.put(Constants.Fields.TRACE_FIELD, trace);

    return new Tuple(objects, "dispatch");
  }

  private long lastSendTime = 0;
  private int count = 0;

  public void progress() {
    if (state != State.WAITING_FOR_READY) {
      LaserScan scan;
      if (simbad) {
        scan = fileIo.read();
      } else {
        scan = dataReader.read();
      }
      if (scan != null) {
        Tuple tuple = createTuple(scan, new Trace());
        execute(tuple);
        count++;
      }
    }
    broadcast.progress();
  }
}
