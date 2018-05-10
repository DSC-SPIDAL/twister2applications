package edu.iu.dsc.tws.apps.slam.core.grid;

import edu.iu.dsc.tws.apps.slam.core.scanmatcher.PointAccumulator;
import edu.iu.dsc.tws.apps.slam.core.utils.DoubleOrientedPoint;
import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;

import java.util.HashSet;
import java.util.Set;

public class GMap implements IGMap {
    private DoublePoint center;
    private double worldSizeX, worldSizeY, delta;
    private HierarchicalArray2D storage;
    private int mapSizeX, mapSizeY;
    private int sizeX2, sizeY2;
    private static final int DEFAULT_PATCH = 5;
    private Set<IntPoint> activeArea = new HashSet<IntPoint>();

    public GMap() {
    }

    public GMap(int mapSizeX, int mapSizeY, double delta) {
        storage = new HierarchicalArray2D(mapSizeX, mapSizeY, DEFAULT_PATCH);
        worldSizeX = mapSizeX * delta;
        worldSizeY = mapSizeY * delta;
        this.delta = delta;
        this.mapSizeX = mapSizeX;
        this.mapSizeY = mapSizeY;
        center = new DoublePoint(0.5 * worldSizeX, 0.5 * worldSizeY);
        sizeX2 = this.mapSizeX >> 1;
        sizeY2 = this.mapSizeY >> 1;
    }

    public GMap(DoublePoint center, double worldSizeX, double worldSizeY, double delta) {
        storage = new HierarchicalArray2D(new Double(Math.ceil(worldSizeX / delta)).intValue(), new Double(Math.ceil(worldSizeY / delta)).intValue(), DEFAULT_PATCH);
        this.center = center;
        this.worldSizeX = worldSizeX;
        this.worldSizeY = worldSizeY;
        this.delta = delta;
        mapSizeX = storage.getXSize() << storage.getPatchSize();
        mapSizeY = storage.getYSize() << storage.getPatchSize();
        sizeX2 = mapSizeX >> 1;
        sizeY2 = mapSizeY >> 1;
    }

    public GMap(DoublePoint center, double xmin, double ymin, double xmax, double ymax, double delta) {
        storage = new HierarchicalArray2D(new Double(Math.ceil((xmax - xmin) / delta)).intValue(), new Double(Math.ceil((ymax - ymin) / delta)).intValue(), DEFAULT_PATCH);
        this.center = center;
        worldSizeX = xmax - xmin;
        worldSizeY = ymax - ymin;
        this.delta = delta;
        mapSizeX = storage.getXSize() << storage.getPatchSize();
        mapSizeY = storage.getYSize() << storage.getPatchSize();
        sizeX2 = (int) Math.round((this.center.x - xmin) / this.delta);
        sizeY2 = (int) Math.round((this.center.y - ymin) / this.delta);
    }

    public void setActiveArea(Set<IntPoint> aa, boolean patchCoords) {
        storage.setActiveArea(aa, patchCoords);
    }

    public void allocActiveArea() {
        storage.allocActiveArea();
    }

    @Override
    public Object cloneStorage() {
        return new HierarchicalArray2D(storage);
    }

    @Override
    public DoublePoint getCenter() {
        return center;
    }

    @Override
    public double getWorldSizeX() {
        return worldSizeX;
    }

    @Override
    public double getMapResolution() {
        return delta;
    }

    @Override
    public double getResolution() {
        return delta;
    }

    @Override
    public Size getSize(double xmin, double ymin, double xmax, double ymax) {
        DoublePoint min = map2world(new IntPoint(0, 0)), max = map2world(new IntPoint(mapSizeX - 1, mapSizeY - 1));
        return new Size(min.x, min.y, max.x, max.y);
    }

    @Override
    public Object cell(int x, int y, boolean c) {
        return cell(new IntPoint(x, y), c);
    }

    @Override
    public Object cell(double x, double y, boolean c) {
        return cell(new IntPoint(new Double(x).intValue(), new Double(y).intValue()), c);
    }

    @Override
    public boolean isInside(int x, int y) {
        return (storage.cellState(new IntPoint(x, y)) & AccessibilityState.Inside.getVal()) > 0;
    }

    @Override
    public boolean isInside(IntPoint p) {
        return (storage.cellState(p) & AccessibilityState.Inside.getVal()) > 0;
    }

    @Override
    public boolean isInsideD(DoublePoint p) {
        return isInside(p.x, p.y);
    }

    @Override
    public boolean isInside(double x, double y) {
        return storage.cellState(
                world2map(new IntPoint(new Double(x).intValue(), new Double(y).intValue())))
                == AccessibilityState.Inside.getVal();
    }


    @Override
    public void resize(double xmin, double ymin, double xmax, double ymax) {
        IntPoint imin = world2map(xmin, ymin);
        IntPoint imax = world2map(xmax, ymax);
        int pxmin, pymin, pxmax, pymax;
        pxmin = (int) Math.floor((double) imin.x / (1 << storage.getPatchMagnitude()));
        pxmax = (int) Math.ceil((double) imax.x / (1 << storage.getPatchMagnitude()));
        pymin = (int) Math.floor((double) imin.y / (1 << storage.getPatchMagnitude()));
        pymax = (int) Math.ceil((double) imax.y / (1 << storage.getPatchMagnitude()));
        storage.resize(pxmin, pymin, pxmax, pymax);
        mapSizeX = storage.getXSize() << storage.getPatchSize();
        mapSizeY = storage.getYSize() << storage.getPatchSize();
        worldSizeX = xmax - xmin;
        worldSizeY = ymax - ymin;
        sizeX2 -= pxmin * (1 << storage.getPatchMagnitude());
        sizeY2 -= pymin * (1 << storage.getPatchMagnitude());
    }

    @Override
    public void grow(double xmin, double ymin, double xmax, double ymax) {
        IntPoint imin = world2map(xmin, ymin);
        IntPoint imax = world2map(xmax, ymax);
        if (isInside(imin) && isInside(imax))
            return;
        imin = IntPoint.min(imin, new IntPoint(0, 0));
        imax = IntPoint.max(imax, new IntPoint(mapSizeX - 1, mapSizeY - 1));
        int pxmin, pymin, pxmax, pymax;
        pxmin = (int) Math.floor((double) imin.x / (1 << storage.getPatchMagnitude()));
        pxmax = (int) Math.ceil((double) imax.x / (1 << storage.getPatchMagnitude()));
        pymin = (int) Math.floor((double) imin.y / (1 << storage.getPatchMagnitude()));
        pymax = (int) Math.ceil((double) imax.y / (1 << storage.getPatchMagnitude()));
        storage.resize(pxmin, pymin, pxmax, pymax);
        mapSizeX = storage.getXSize() << storage.getPatchSize();
        mapSizeY = storage.getYSize() << storage.getPatchSize();
        worldSizeX = xmax - xmin;
        worldSizeY = ymax - ymin;
        sizeX2 -= pxmin * (1 << storage.getPatchMagnitude());
        sizeY2 -= pymin * (1 << storage.getPatchMagnitude());
    }

    @Override
    public IntPoint world2map(double x, double y) {
        return new IntPoint((int) Math.round((x - center.x) / delta) + sizeX2,
                (int) Math.round((y - center.y) / delta) + sizeY2);
    }

    @Override
    public IntPoint world2map(IntPoint p) {
        return new IntPoint((int) Math.round((p.x - center.x) / delta) + sizeX2,
                (int) Math.round((p.y - center.y) / delta) + sizeY2);
    }

    @Override
    public IntPoint world2map(DoublePoint p) {
        return new IntPoint((int) Math.round((p.x - center.x) / delta) + sizeX2,
                (int) Math.round((p.y - center.y) / delta) + sizeY2);
    }

    @Override
    public IntPoint world2map(DoubleOrientedPoint p) {
        return new IntPoint((int) Math.round((p.x - center.x) / delta) + sizeX2,
                (int) Math.round((p.y - center.y) / delta) + sizeY2);
    }

    @Override
    public DoublePoint map2world(IntPoint p) {
        return new DoublePoint((p.x - sizeX2) * delta + center.x,
                (p.y - sizeY2) * delta + center.y);
    }

    @Override
    public Object cell(IntPoint p, boolean c) {
        int s = storage.cellState(p);
        if (c) {
            if ((s & 0x2) > 0)
                return storage.cell(p);
        } else {
            if ((s & 0x1) == 0)
                assert false;
            return storage.cell(p);
        }

        return new PointAccumulator();
    }

    public Object cell(DoublePoint p, boolean c) {
        IntPoint ip = world2map(p);
        int s = storage.cellState(ip);
        //if (! s&Inside) assert(0);
        if (c) {
            if ((s & AccessibilityState.Allocated.getVal()) > 0)
                return storage.cell(ip);
        } else {
            if ((s & AccessibilityState.Inside.getVal()) == 0)
                assert false;
            return storage.cell(ip);
        }
        return new PointAccumulator();
    }

    @Override
    public DoublePoint getM_center() {
        return center;
    }

    @Override
    public double getWorldSizeY() {
        return worldSizeY;
    }

    @Override
    public double getDelta() {
        return delta;
    }

    @Override
    public HierarchicalArray2D getStorage() {
        return storage;
    }

    @Override
    public int getMapSizeX() {
        return mapSizeX;
    }

    @Override
    public int getMapSizeY() {
        return mapSizeY;
    }

    @Override
    public int getSizeX2() {
        return sizeX2;
    }

    @Override
    public int getSizeY2() {
        return sizeY2;
    }

    @Override
    public void setM_center(DoublePoint m_center) {
        this.center = m_center;
    }

    @Override
    public void setWorldSizeX(double worldSizeX) {
        this.worldSizeX = worldSizeX;
    }

    @Override
    public void setWorldSizeY(double worldSizeY) {
        this.worldSizeY = worldSizeY;
    }

    @Override
    public void setDelta(double delta) {
        this.delta = delta;
    }

    @Override
    public void setStorage(Object storage) {
        if (storage instanceof HierarchicalArray2D) {
            this.storage = (HierarchicalArray2D) storage;
        } else {
            throw new IllegalArgumentException("HierarchicalArray2D expected");
        }
    }

    public Set<IntPoint> getActiveArea() {
        return activeArea;
    }

    @Override
    public int getPatchSize() {
        return storage.getPatchSize();
    }

    @Override
    public int getPatchMagnitude() {
        return storage.getPatchMagnitude();
    }

    @Override
    public void setMapSizeX(int mapSizeX) {
        this.mapSizeX = mapSizeX;
    }

    @Override
    public void setMapSizeY(int mapSizeY) {
        this.mapSizeY = mapSizeY;
    }

    @Override
    public void setSizeX2(int sizeX2) {
        this.sizeX2 = sizeX2;
    }

    @Override
    public void setSizeY2(int sizeY2) {
        this.sizeY2 = sizeY2;
    }
}
