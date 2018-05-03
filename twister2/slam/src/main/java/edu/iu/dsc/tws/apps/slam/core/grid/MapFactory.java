package edu.iu.dsc.tws.apps.slam.core.grid;

import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;

public class MapFactory {
    public static IGMap create(DoublePoint center, double worldSizeX,
                               double worldSizeY, double delta) {
        return new StaticMap(center, worldSizeX, worldSizeY, delta);
    }

    public static IGMap create(DoublePoint center, double xmin, double ymin,
                               double xmax, double ymax, double delta) {
        return new StaticMap(center, xmin, xmax, ymin, ymax, delta);
    }
}
