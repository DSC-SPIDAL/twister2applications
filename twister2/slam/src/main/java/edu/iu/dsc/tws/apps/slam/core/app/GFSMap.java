package edu.iu.dsc.tws.apps.slam.core.app;

import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is the map output by the MapUpdater
 */
public class GFSMap {
    public int width;
    public int height;

    public double resolution;

    public int data[];

    public List<IntPoint> currentPos = new ArrayList<IntPoint>();

    Position origin = new Position();

    Position originOrientation = new Position();

    public GFSMap() {
    }

    public GFSMap(int width, int height, double resolution) {
        this.width = width;
        this.height = height;
        this.resolution = resolution;
    }

    public GFSMap(int width, int height) {
        this.width = width;
        this.height = height;

        data = new int[width * height];
    }

    public void resize(int size) {
        data = new int[size];
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public double getResolution() {
        return resolution;
    }

    public int[] getData() {
        return data;
    }

    public List<IntPoint> getCurrentPos() {
        return currentPos;
    }

    public Position getOrigin() {
        return origin;
    }

    public Position getOriginOrientation() {
        return originOrientation;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public void setResolution(double resolution) {
        this.resolution = resolution;
    }

    public void setData(int[] data) {
        this.data = data;
    }

    public void setCurrentPos(List<IntPoint> currentPos) {
        this.currentPos = currentPos;
    }

    public void setOrigin(Position origin) {
        this.origin = origin;
    }

    public void setOriginOrientation(Position originOrientation) {
        this.originOrientation = originOrientation;
    }
}
