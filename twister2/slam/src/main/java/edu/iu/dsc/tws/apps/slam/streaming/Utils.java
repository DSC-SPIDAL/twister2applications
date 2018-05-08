package edu.iu.dsc.tws.apps.slam.streaming;

import edu.iu.dsc.tws.apps.slam.core.app.LaserScan;
import edu.iu.dsc.tws.apps.slam.core.app.Position;
import edu.iu.dsc.tws.apps.slam.core.grid.*;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.Particle;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.TNode;
import edu.iu.dsc.tws.apps.slam.core.scanmatcher.PointAccumulator;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeSensor;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;
import edu.iu.dsc.tws.apps.slam.streaming.msgs.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 * Utilities
 */
public class Utils {
  private static Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Serialize an object using kryo and return the bytes
   *
   * @param kryo   instance of kryo
   * @param object the object to be serialized
   * @return the serialized bytes
   */
  public static byte[] serialize(Kryo kryo, Object object) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Output output = new Output(byteArrayOutputStream);
//        output.setOutputStream(byteArrayOutputStream);
    kryo.writeObject(output, object);
    output.flush();
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * De Serialize bytes using kryo and return the object
   *
   * @param kryo instance of kryo
   * @param b    the byte to be de serialized
   * @return the serialized bytes
   */
  public static Object deSerialize(Kryo kryo, byte[] b, Class e) {
    return kryo.readObject(new Input(new ByteArrayInputStream(b)), e);
  }

  public static TransferMap createTransferMap(IGMap map) {
    TransferMap transferMap = new TransferMap(map.getCenter(), map.getWorldSizeX(),
        map.getWorldSizeY(), map.getDelta(), map.getMapSizeX(),
        map.getMapSizeY(), map.getSizeX2(), map.getSizeY2(),
        map.getPatchMagnitude(),
        map.getPatchSize());
//        LOG.info("Transfer map x {} y {}", map.getMapSizeX(), map.getMapSizeY());
    int sizeX = map.getMapSizeX();
    int sizeY = map.getMapSizeY();
    for (int x = 0; x < sizeX; x++) {
      for (int y = 0; y < sizeY; y++) {
        /// @todo Sort out the unknown vs. free vs. obstacle thresholding
        IntPoint p = new IntPoint(x, y);
        PointAccumulator pointAccumulator = (PointAccumulator) map.cell(p, false);
        if (pointAccumulator.getVisits() > 0) {
          MapCell cell = createMapCell(pointAccumulator, x, y);
          transferMap.addCell(cell);
        }
      }
    }

    transferMap.setActiveArea(map.getActiveArea());
    return transferMap;
  }

  public static IGMap createGMap(TransferMap tMap) {
    IGMap gMap = MapFactory.create(tMap.getCenter(), tMap.getWorldSizeX(), tMap.getWorldSizeY(), tMap.getDelta());

    for (MapCell cell : tMap.getMapCells()) {
      PointAccumulator accumulator = (PointAccumulator) gMap.cell(cell.getX(), cell.getY(), false);
      if (accumulator != null) {
        accumulator.setAcc(cell.getAccx(), cell.getAccy());
        accumulator.setN(cell.getN());
        accumulator.setVisits(cell.getVisits());
      }
    }

    if (tMap.getActiveArea() != null) {
      gMap.setActiveArea(tMap.getActiveArea(), true);
    }

    return gMap;
  }

  public static IGMap createGMap(TransferMap tMap, int type) {
    IGMap gMap;
    if (type == IGMap.HIERARCHICAL_MAP) {
      gMap = MapFactory.create(tMap.getCenter(), tMap.getWorldSizeX(), tMap.getWorldSizeY(), tMap.getDelta());
    } else {
      gMap = MapFactory.create(tMap.getCenter(), tMap.getWorldSizeX(), tMap.getWorldSizeY(), tMap.getDelta());
    }

    for (MapCell cell : tMap.getMapCells()) {
      PointAccumulator accumulator = (PointAccumulator) gMap.cell(cell.getX(), cell.getY(), false);
      accumulator.setAcc(cell.getAccx(), cell.getAccy());
      accumulator.setN(cell.getN());
      accumulator.setVisits(cell.getVisits());
    }

    if (tMap.getActiveArea() != null) {
      gMap.setActiveArea(tMap.getActiveArea(), true);
    }

    return gMap;
  }

  public static MapCell createMapCell(PointAccumulator acc, int x, int y) {
    return new MapCell(x, y, acc.getAccx(), acc.getAccy(), acc.getN(), acc.getVisits());
  }

  public static void createParticle(ParticleValue value, Particle p, boolean withNode) {
    p.setPose(value.getPose());
    p.setWeightSum(value.getWeightSum());
    p.setWeight(value.getWeight());
    p.setPreviousIndex(value.getPreviousIndex());
    p.setGweight(value.getGweight());
    p.setPreviousPose(value.getPreviousPose());
    if (withNode) {
      p.setNode(createNodeFromList(value.getNodes()));
    }
  }

  public static void createParticle(ParticleValue value, Particle p) {
    p.setPose(value.getPose());
    p.setWeightSum(value.getWeightSum());
    p.setWeight(value.getWeight());
    p.setPreviousIndex(value.getPreviousIndex());
    p.setGweight(value.getGweight());
    p.setPreviousPose(value.getPreviousPose());
    p.setNode(createNodeFromList(value.getNodes()));
  }

  public static ParticleValue createParticleValue(Particle p, int taskId, int index, int totalTasks) {
    ParticleValue pv = new ParticleValue(taskId, index, totalTasks, p.getPose(), p.getPreviousPose(),
        p.getWeight(), p.getWeightSum(), p.getGweight(), p.getPreviousIndex(), createNodeListFromNodeTree(p.getNode()));
    return pv;
  }

  public static ParticleValue createParticleValue(Particle p, int taskId, int index, int totalTasks, List<RangeReading> readings) {
    ParticleValue pv = new ParticleValue(taskId, index, totalTasks, p.getPose(), p.getPreviousPose(),
        p.getWeight(), p.getWeightSum(), p.getGweight(), p.getPreviousIndex(), createNodeListFromNodeTree(p.getNode(), readings));
    return pv;
  }

  public static List<TNodeValue> createNodeListFromNodeTree(TNode node, List<RangeReading> readings) {
    List<TNodeValue> values = new ArrayList<TNodeValue>();
    int i = readings.size() - 1;
    TNode n = node;
    while (n != null) {
      TNodeValue e;
      if (i >= 0) {
        e = new TNodeValue(n.getPose(), n.getWeight(), n.getAccWeight(), n.getGweight(), readings.get(i), n.getChilds(), n.getVisitCounter(), n.isFlag());
      } else {
        e = new TNodeValue(n.getPose(), n.getWeight(), n.getAccWeight(), n.getGweight(), n.getReading(), n.getChilds(), n.getVisitCounter(), n.isFlag());
      }

      i--;

      values.add(e);
      n = n.getParent();
    }

    return values;
  }

  public static List<TNodeValue> createNodeListFromNodeTree(TNode node) {
    List<TNodeValue> values = new ArrayList<TNodeValue>();

    TNode n = node;
    while (n != null) {
      values.add(new TNodeValue(n.getPose(), n.getWeight(), n.getAccWeight(), n.getGweight(), null, n.getChilds(), n.getVisitCounter(), n.isFlag()));
      n = n.getParent();
    }

    return values;
  }

  public static TNode createNodeFromList(List<TNodeValue> list) {
    TNode next = null;
    if (list.size() > 0) {
      for (int i = list.size() - 1; i >= 0; i--) {
        TNodeValue v = list.get(i);
        next = new TNode(v.getPose(), v.getWeight(), v.getAccWeight(), v.getGweight(), next, v.getReading(), v.getChilds(), v.getVisitCounter(), v.isFlag());
      }
    } else {
      next = new TNode(new DoubleOrientedPoint(0, 0, 0), 0, 0, 0, null, null, 0, 0, false);
    }
    return next;
  }

  public static void registerClasses(Kryo kryo) {
    kryo.register(DoublePoint.class);
    kryo.register(IntPoint.class);
    kryo.register(Particle.class);
    kryo.register(GMap.class);
    kryo.register(Array2D.class);
    kryo.register(HierarchicalArray2D.class);
    kryo.register(TNode.class);
    kryo.register(DoubleOrientedPoint.class);
    kryo.register(Particle.class);
    kryo.register(PointAccumulator.class);
    kryo.register(HierarchicalArray2D.class);
    kryo.register(Array2D.class);
    kryo.register(Position.class);
    kryo.register(Object[][].class);
    kryo.register(TransferMap.class);
    kryo.register(ParticleMaps.class);
    kryo.register(MapCell.class);
    kryo.register(TNodeValue.class);
    kryo.register(ParticleAssignment.class);
    kryo.register(ParticleAssignments.class);
    kryo.register(LaserScan.class);
    kryo.register(ParticleValue.class);
    kryo.register(RangeSensor.class);
    kryo.register(RangeReading.class);
    kryo.register(ParticleValues.class);
    kryo.register(ParticleMapsList.class);
    kryo.register(StaticMap.class);
    kryo.register(Ready.class);
    kryo.register(Trace.class);
  }

  public static TaskPlan createReduceTaskPlan(Config cfg, ResourcePlan plan, int parallel) {
    int noOfContainers = plan.noOfContainers();

    if (noOfContainers != parallel + 1) {
      throw new RuntimeException("Invalid arguments");
    }

    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = plan.getThisId();

    List<ResourceContainer> containers = plan.getContainers();
    Map<String, List<ResourceContainer>> containersPerNode = ResourcePlanUtils.getContainersPerNode(containers);

    int totalTasksPreviously = 0;
    for (int stage = 0; stage < 2; stage++) {
      for (int taskId = 0; taskId < parallel; taskId++) {
        Set<Integer> nodesOfExecutor;
        if (executorToGraphNodes.get(taskId) == null) {
          nodesOfExecutor = new HashSet<>();
          executorToGraphNodes.put(taskId, nodesOfExecutor);
        } else {
          nodesOfExecutor = executorToGraphNodes.get(taskId);
        }
        nodesOfExecutor.add(totalTasksPreviously + stage * parallel + taskId);
      }
    }
    Set<Integer> dispatcherExec = new HashSet<>();
    dispatcherExec.add(2 * parallel);
    executorToGraphNodes.put(parallel, dispatcherExec);

    int i = 0;
    for (Map.Entry<String, List<ResourceContainer>> entry : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (ResourceContainer c : entry.getValue()) {
        executorsOfGroup.add(c.getId());
      }
      groupsToExeuctors.put(i, executorsOfGroup);
      i++;
    }

    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }
}
