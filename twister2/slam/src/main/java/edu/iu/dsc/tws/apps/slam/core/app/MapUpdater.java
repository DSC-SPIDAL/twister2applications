package edu.iu.dsc.tws.apps.slam.core.app;

import edu.iu.dsc.tws.apps.slam.core.grid.IGMap;
import edu.iu.dsc.tws.apps.slam.core.grid.MapFactory;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.Particle;
import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.TNode;
import edu.iu.dsc.tws.apps.slam.core.scanmatcher.PointAccumulator;
import edu.iu.dsc.tws.apps.slam.core.scanmatcher.ScanMatcher;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapUpdater {
    private Logger LOG = LoggerFactory.getLogger(MapUpdater.class);
    boolean got_map_;

    // Parameters used by GMapping
    double maxRange_;
    double maxUrange_;

    double xmin;
    double ymin;
    double xmax;
    double ymax;
    double delta;
    double occThresh;
    private GFSMap map = new GFSMap();

    public MapUpdater(double maxRange, double maxUrange, double xmin, double ymin, double xmax, double ymax, double delta, double occThresh) {
        this.maxRange_ = maxRange;
        this.maxUrange_ = maxUrange;
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
        this.delta = delta;
        this.occThresh = occThresh;
    }

    public GFSMap getMap() {
        return map;
    }

    public GFSMap updateMap(Particle best, double angles[], DoubleOrientedPoint laserPose) {
        ScanMatcher matcher = new ScanMatcher();
        matcher.setLaserParameters(angles.length, angles, laserPose);
        LOG.info("best particle pose: {}", best.getPose());
        matcher.setLaserMaxRange(maxRange_);
        matcher.setUsableRange(maxUrange_);
        matcher.setgenerateMap(true);

        if (!got_map_) {
            map.resolution = delta;
            map.origin.x = 0.0;
            map.origin.y = 0.0;
            map.origin.z = 0.0;
            map.originOrientation.x = 0.0;
            map.originOrientation.y = 0.0;
            map.originOrientation.z = 0.0;
            map.originOrientation.w = 1.0;
        }

        DoublePoint center = new DoublePoint((xmin + xmax) / 2.0, (ymin + ymax) / 2.0);
        IGMap smap = MapFactory.create(center, xmax - xmin, ymax - ymin, delta);
        best.setMap(smap);
        map.currentPos.clear();

        LOG.debug("Trajectory tree:");
        synchronized (map.currentPos) {
            for (TNode n = best.node; n != null; n = n.parent) {
                LOG.debug("{} {} {}", n.pose.x, n.pose.y, n.pose.theta);
                if (n.reading == null) {
                    LOG.debug("Reading is NULL");
                    continue;
                }

                IntPoint p = best.map.world2map(n.pose);
                map.currentPos.add(p);

                matcher.invalidateActiveArea();
                double[] readingArray = new double[n.reading.size()];
                //System.out.format("best pose: (%f %f %f) reading: (", n.pose.x, n.pose.y, n.pose.theta);
                for (int i = 0; i < n.reading.size(); i++) {
                    readingArray[i] = n.reading.get(i);
                    //System.out.format("%f ", readingArray[i]);
                }
                //System.out.format(")\n");

                matcher.computeActiveArea(smap, n.pose, readingArray);
                matcher.registerScan(smap, n.pose, readingArray);
            }
        }

        // the map may have expanded, so resize ros message as well
        if (map.width != smap.getMapSizeX() || map.height != smap.getMapSizeY()) {

            // NOTE: The results of ScanMatcherMap::getSize() are different from the parameters given to the constructor
            // so we must obtain the bounding box in a different way
            DoublePoint wmin = smap.map2world(new IntPoint(0, 0));
            DoublePoint wmax = smap.map2world(new IntPoint(smap.getMapSizeX(), smap.getMapSizeY()));

            ymin = wmin.y;
            xmax = wmax.x;
            ymax = wmax.y;

            LOG.info("map size is now {} x {} pixels ({},{})-({}, {})", smap.getMapSizeX(), smap.getMapSizeY(),
                    xmin, ymin, xmax, ymax);

            map.width = smap.getMapSizeX();
            map.height = smap.getMapSizeY();
            map.origin.x = xmin;
            map.origin.y = ymin;
            map.resize(map.width * map.height);
            LOG.debug("map origin: (%f, %f)", map.origin.x, map.origin.y);
        }
        for (int x = 0; x < smap.getMapSizeX(); x++) {
            for (int y = 0; y < smap.getMapSizeY(); y++) {
                /// @todo Sort out the unknown vs. free vs. obstacle thresholding
                PointAccumulator pointAccumulator = (PointAccumulator) smap.cell(x, y, false);
                double occ = -1;
                if (pointAccumulator != null) {
                    occ = pointAccumulator.doubleValue();
                }
                assert (occ <= 1.0);
                //System.out.println("threshold: "  + occThresh  + " occ: "  + occ);
                if (occ < 0) {
                    map.data[MAP_IDX(map.width, x, y)] = -1;
                } else if (occ > occThresh) {
                    //map.map.data[MAP_IDX(map.map.info.width, x, y)] = (int)round(occ*100.0);
                    map.data[MAP_IDX(map.width, x, y)] = 100;
                } else {
                    map.data[MAP_IDX(map.width, x, y)] = 0;
                }
            }
        }
        got_map_ = true;
        return map;
    }

    public static int MAP_IDX(int sx, int i, int j) {
        return ((sx) * (j) + (i));
    }
}
