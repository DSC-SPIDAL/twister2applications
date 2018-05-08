package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.Ready;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.Trace;
import com.esotericsoftware.kryo.Kryo;
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

  private Kryo kryo;
  private Kryo mainKryo;

  private Map conf;

  private int noOfParallelTasks = 0;

  private Lock lock = new ReentrantLock();

  private Intracomm intracomm;

  private SlamDataReader dataReader;

  private KryoMemorySerializer kryoMemorySerializer;

  private int task;

  private enum State {
    WAITING_FOR_READING,
    WAITING_FOR_READY,
  }

  private State state = State.WAITING_FOR_READING;

  public void prepare(Map stormConf, Intracomm intracomm, String inputFile, int task) {
    this.conf = stormConf;
    this.intracomm = intracomm;
    this.task = task;

    // todo: set this value
    this.noOfParallelTasks = 0;

    kryo = new Kryo();
    mainKryo = new Kryo();
    Utils.registerClasses(kryo);
    Utils.registerClasses(mainKryo);
    this.dataReader = new SlamDataReader(inputFile);
    this.kryoMemorySerializer = new KryoMemorySerializer();
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
        broadcast.send(task, input, 0);
        this.state = State.WAITING_FOR_READY;
        LOG.info("Changing state from READING to ANY");
      } else if (this.state == State.WAITING_FOR_READY) {
        LOG.info("Input while in state READY");
      }
    }  finally {
      lock.unlock();
    }
  }


  private void handleReady(Ready ready) {
    lock.lock();
    try {
      state = State.WAITING_FOR_READING;
      LOG.info("Changing state from ANY to READING");
    } finally {
      lock.unlock();
    }
  }

  private Tuple createTuple(LaserScan scan, Trace trace) {
    Map<String, Object> objects = new HashMap<>();
    objects.put(Constants.Fields.BODY, kryoMemorySerializer.serialize(scan));
    objects.put(Constants.Fields.TIME_FIELD, System.currentTimeMillis());
    objects.put(Constants.Fields.SENSOR_ID_FIELD, "");

    byte[] traceBytes = Utils.serialize(mainKryo, trace);
    objects.put(Constants.Fields.TRACE_FIELD, traceBytes);

    return new Tuple(objects, "dispatch");
  }

  public void progress() {
    if (state != State.WAITING_FOR_READY) {
      LaserScan scan = dataReader.read();
      if (scan != null) {
        Tuple tuple = createTuple(scan, new Trace());
        execute(tuple);
      }
    }
    broadcast.progress();
  }
}
