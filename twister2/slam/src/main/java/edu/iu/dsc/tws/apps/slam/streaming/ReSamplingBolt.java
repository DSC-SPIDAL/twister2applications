package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.GFSConfiguration;
import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.Particle;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.ReSampler;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeSensor;
import edu.iu.dsc.tws.apps.slam.core.sensor.Sensor;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.*;
import com.esotericsoftware.kryo.Kryo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReSamplingBolt {
    private static Logger LOG = LoggerFactory.getLogger(ReSamplingBolt.class);

    /** We will re sample here */
    private DistributedReSampler reSampler;

    /** we will collect the values until we get all of them */
    private ParticleValue []particleValueses;

    private Map<Integer, Trace> traceMap = new HashMap<Integer, Trace>();

    /** This is the reading message we will get */
    private RangeReading reading;

    private String url = "amqp://localhost:5672";

    private Kryo kryo;

    private int receivedParticles = 0;
    private Map conf;

    private long firstReadingTime = -1;

    private ExecutorService executor;
    private List<Kryo> kryoValueWriters = new ArrayList<Kryo>();

    private Lock lock = new ReentrantLock();

    public void prepare(Map map) {
        this.conf = map;
        this.executor = Executors.newScheduledThreadPool(8);
        this.kryo = new Kryo();
        Utils.registerClasses(kryo);
        // read the configuration of the scanmatcher from topology.xml
        // use the configuration to create the resampler
        init(conf);
        // todo
        int totalTasks = 0;
        this.url = null;
        try {
            for (int i = 0; i < totalTasks; i++) {
                Kryo k = new Kryo();
                Utils.registerClasses(k);
                kryoValueWriters.add(k);
            }
        } catch (Exception e) {
            LOG.error("Failed to create the sender", e);
        }
    }

    /**
     * Initialize the resampler
     */
    private void init(Map conf) {
        LOG.info("Initializing resampling bolt");
        GFSConfiguration cfg = ConfigurationBuilder.getConfiguration(conf);
        if (conf.get(Constants.ARGS_PARTICLES) != null) {
            cfg.setNoOfParticles(((Long) conf.get(Constants.ARGS_PARTICLES)).intValue());
        }
        reSampler = ProcessorFactory.createReSampler(cfg);
        particleValueses = new ParticleValue[reSampler.getNoOfParticles()];
    }

    public void execute(Tuple tuple) {
        String stream = tuple.getSourceStreamId();
        // if we receive a control message init and return
        if (stream.equals(Constants.Fields.CONTROL_STREAM)) {
            init(conf);
            return;
        }

        if (firstReadingTime == -1) {
            firstReadingTime = System.currentTimeMillis();
        }

        lock.lock();
        try {
            Trace trace = (Trace) tuple.getValueByField(Constants.Fields.TRACE_FIELD);
            Object val = tuple.getValueByField(Constants.Fields.PARTICLE_VALUE_FIELD);
            List<ParticleValue> pvs;

            if (val != null && (val instanceof List)) {
                pvs = (List<ParticleValue>) val;
                for (ParticleValue value : pvs) {
                    LOG.debug("Received particle with index {}", value.getIndex());
                    addParticleValue(value, trace);
                }
            } else {
                throw new IllegalArgumentException("The particle value should be of type ParticleValue");
            }

            val = tuple.getValueByField(Constants.Fields.LASER_SCAN_FIELD);
            if (val != null && !(val instanceof LaserScan)) {
                throw new IllegalArgumentException("The laser scan should be of type LaserScan");
            }

            LaserScan scan = (LaserScan) val;


            Double[] ranges_double = edu.iu.dsc.tws.apps.slam.core.utils.Utils.getRanges(scan, scan.getAngleIncrement());
            RangeSensor sensor = new RangeSensor("ROBOTLASER1",
                    scan.getRanges().size(),
                    Math.abs(scan.getAngleIncrement()),
                    new DoubleOrientedPoint(0, 0, 0),
                    0.0,
                    scan.getRangeMax());

            reading = new RangeReading(scan.getRanges().size(),
                    ranges_double,
                    scan.getTimestamp());
            reading.setPose(scan.getPose());
            Map<String, Sensor> smap = new HashMap<String, Sensor>();
            smap.put(sensor.getName(), sensor);

            LOG.debug("receivedParticles: {}, expecting particles:{}", receivedParticles, reSampler.getParticles().size());
            // this bolt will wait until all the particle values are obtained
            if (receivedParticles < reSampler.getNoOfParticles() || reading == null) {
                return;
            }
        } finally {
            lock.unlock();
        }
        // reset the counter
        receivedParticles = 0;
        long resamplingStartTime =  System.currentTimeMillis();
        // now distribute the particle Valueses to the bolts
        // we got all the particleValueses, we will resample
        // first we need to clear the current particleValueses
        reSampler.getParticles().clear();
        for (ParticleValue pv : particleValueses) {
            Particle p = new Particle();
            Utils.createParticle(pv, p);

            reSampler.getParticles().add(pv.getIndex(), p);
        }

        // do the resampling
        ReSampler.ReSampleResult hasReSampled = reSampler.processScan(reading, 0);
        // now distribute the resampled particleValueses
        long resampleTime = System.currentTimeMillis() - resamplingStartTime;

        int best = reSampler.getBestParticleIndex();
        // we will distribute only if we have reSampled
        if (hasReSampled.isReSampled()) {
            // first we will distribute the new assignments
            // this will distribute the current maps
            LOG.info("ReSampled, distributing assignments.....");
            List<Integer> particles = hasReSampled.getIndexes();
            ParticleAssignments assignments = createAssignments(particles);
            assignments.setReSampled(true);
            assignments.setBestParticle(best);
            distributeAssignments(assignments, resampleTime);

            Map<Integer, List<ParticleValue>> values = new HashMap<Integer, List<ParticleValue>>();
            // distribute the new particle values according to
            for (int i = 0; i < reSampler.getParticles().size(); i++) {
                Particle p = reSampler.getParticles().get(i);
                ParticleValue pv = Utils.createParticleValue(p, -1, i, -1);

                // we assume there is a direct mapping between particles in the resampler and the indexes
                ParticleAssignment assignment = assignments.getAssignments().get(i);
                if (i == best) {
                    LOG.info("Best node index: {}, sending this to task: {}", i, assignment.getNewTask());
                    pv.setBest(true);
                }
                addParticleValueToMap(values, assignment.getNewTask(), pv);
            }

            for (final Map.Entry<Integer, List<ParticleValue>> e : values.entrySet()) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ParticleValues particleValues = new ParticleValues(e.getValue());
                            Kryo kryo = kryoValueWriters.get(e.getKey());
                            byte[] b = Utils.serialize(kryo, particleValues);
                            Message message = new Message(b);
                            LOG.info("Sending particle value to: {}", e.getKey());
                            // todo
//                            valueSender.send(message, Constants.Messages.PARTICLE_VALUE_ROUTING_KEY + "_" + e.getKey());
                        } catch (Exception e1) {
                            LOG.error("Failed to send the message", e1);
                        }
                    }
                });
            }
        } else {
            LOG.info("NOT ReSampled, distributing assignments");
            ParticleAssignments assignments = new ParticleAssignments();
            assignments.setReSampled(false);
            assignments.setBestParticle(best);
            distributeAssignments(assignments, resampleTime);
        }

        reading = null;
    }

    private void addParticleValueToMap(Map<Integer, List<ParticleValue>> map, int task, ParticleValue value) {
        List<ParticleValue> values;
        if (map.containsKey(task)) {
            values = map.get(task);
        } else {
            values = new ArrayList<ParticleValue>();
            map.put(task, values);
        }
        values.add(value);
    }

    protected synchronized void addParticleValue(ParticleValue value, Trace trace) {
        LOG.debug("Received particle value from taskID {}", value.getTaskId());
        particleValueses[value.getIndex()] = value;
        traceMap.put(value.getTaskId(), trace);
        receivedParticles++;
    }

    /**
     * We will broadcast this message using a topic. Every ScanMatching bolt will receive this message
     * @param assignments the particle assignments
     */
    private void distributeAssignments(ParticleAssignments assignments, long time) {
        Trace t = new Trace();
        // gather the traces
        for (Map.Entry<Integer, Trace> e : traceMap.entrySet()) {
            t.getSmp().put(e.getKey(), e.getValue().getSmp().get(e.getKey()));
            t.getGcTimes().put(e.getKey(), e.getValue().getGcTimes().get(e.getKey()));
            t.setPd(e.getValue().getPd());
        }
        t.setActualRsp(time);
        t.setRsp(System.currentTimeMillis() - firstReadingTime);

        firstReadingTime = -1;
        assignments.setTrace(t);

        LOG.debug("Sending particle assignment");
        byte []b = Utils.serialize(kryo, assignments);
        List<Object> emit = new ArrayList<Object>();
        emit.add(b);
        // todo
//        outputCollector.emit(Constants.Fields.ASSIGNMENT_STREAM, emit);
    }

    /**
     * This method create an assignment of the resampled particles to the tasks running in Storm.
     * In this case the tasks wille.printStackTrace(); be the bolts running the ScanMatching code.
     * @param indexes the re sampled indexes
     * @return an assignment of particles
     */
    protected ParticleAssignments createAssignments(List<Integer> indexes) {
        // create a matrix of size noOfParticles x noOfparticles
        int noOfParticles = reSampler.getNoOfParticles();
        // assume taskIndexes are going from 0
        double [][]cost = new double[noOfParticles][noOfParticles];
        for (int i = 0; i < noOfParticles; i++) {
            cost[i] = new double[noOfParticles];
            for (int j = 0; j < noOfParticles; j++) {
                int index = indexes.get(j);
                ParticleValue pv = particleValueses[index];
                // now see weather this particle is from this worker
                int particleTaskIndex = pv.getTaskId();
                int thrueTaskIndex = i % pv.getTotalTasks();
                if (particleTaskIndex == thrueTaskIndex) {
                    cost[i][j] = 0;
                } else {
                    cost[i][j] = 1;
                }
            }
        }

        HungarianAlgorithm algorithm = new HungarianAlgorithm(cost);
        int []assignments = algorithm.execute();
        ParticleAssignments particleAssignments = new ParticleAssignments();

        // go through the particle indexs and try to find their new assignments
        for (int i = 0; i < indexes.size(); i++) {
            int particle = indexes.get(i);
            int thrueTaskIndex = -1;
            ParticleValue pv = particleValueses[particle];
            for (int j = 0; j < assignments.length; j++) {
                if (assignments[j] == i) {
                    thrueTaskIndex = j % pv.getTotalTasks();
                    break;
                }
            }

            ParticleAssignment assignment = new ParticleAssignment(particle, i,
                    pv.getTaskId(), thrueTaskIndex);
            particleAssignments.addAssignment(assignment);
        }
        return particleAssignments;
    }
}
