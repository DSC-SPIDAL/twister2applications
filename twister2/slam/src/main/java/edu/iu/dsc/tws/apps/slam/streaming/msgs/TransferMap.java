package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import edu.iu.dsc.tws.apps.slam.core.utils.DoublePoint;
import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransferMap {
    // the actual map cells
    private List<MapCell> mapCells = new ArrayList<MapCell>();

    // the map properties
    private DoublePoint center;
    private double worldSizeX, worldSizeY, delta;
    private int mapSizeX, mapSizeY;
    private int sizeX2, sizeY2;

    // hierachical array properties
    Set<IntPoint> activeArea = new HashSet<IntPoint>();
    int patchMagnitude = 0;
    int patchSize;

    public TransferMap() {
    }

    public TransferMap(DoublePoint center, double worldSizeX,
                       double worldSizeY, double delta,
                       int mapSizeX, int mapSizeY, int sizeX2,
                       int sizeY2, int patchMagnitude, int patchSize) {
        this.center = center;
        this.worldSizeX = worldSizeX;
        this.worldSizeY = worldSizeY;
        this.delta = delta;
        this.mapSizeX = mapSizeX;
        this.mapSizeY = mapSizeY;
        this.sizeX2 = sizeX2;
        this.sizeY2 = sizeY2;
        this.patchMagnitude = patchMagnitude;
        this.patchSize = patchSize;
    }

    public List<MapCell> getMapCells() {
        return mapCells;
    }

    public void setMapCells(List<MapCell> mapCells) {
        this.mapCells = mapCells;
    }

    public void addCell(MapCell cell) {
        mapCells.add(cell);
    }

    public DoublePoint getCenter() {
        return center;
    }

    public double getWorldSizeX() {
        return worldSizeX;
    }

    public double getWorldSizeY() {
        return worldSizeY;
    }

    public double getDelta() {
        return delta;
    }

    public int getMapSizeX() {
        return mapSizeX;
    }

    public int getMapSizeY() {
        return mapSizeY;
    }

    public int getSizeX2() {
        return sizeX2;
    }

    public int getSizeY2() {
        return sizeY2;
    }

    public void setCenter(DoublePoint center) {
        this.center = center;
    }

    public void setWorldSizeX(double worldSizeX) {
        this.worldSizeX = worldSizeX;
    }

    public void setWorldSizeY(double worldSizeY) {
        this.worldSizeY = worldSizeY;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }

    public void setMapSizeX(int mapSizeX) {
        this.mapSizeX = mapSizeX;
    }

    public void setMapSizeY(int mapSizeY) {
        this.mapSizeY = mapSizeY;
    }

    public void setSizeX2(int sizeX2) {
        this.sizeX2 = sizeX2;
    }

    public void setSizeY2(int sizeY2) {
        this.sizeY2 = sizeY2;
    }

    public Set<IntPoint> getActiveArea() {
        return activeArea;
    }

    public int getPatchMagnitude() {
        return patchMagnitude;
    }

    public int getPatchSize() {
        return patchSize;
    }

    public void setActiveArea(Set<IntPoint> activeArea) {
        this.activeArea = activeArea;
    }

    public void setPatchMagnitude(int patchMagnitude) {
        this.patchMagnitude = patchMagnitude;
    }

    public void setPatchSize(int patchSize) {
        this.patchSize = patchSize;
    }
}
