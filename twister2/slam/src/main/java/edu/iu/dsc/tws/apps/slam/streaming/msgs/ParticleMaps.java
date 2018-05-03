package edu.iu.dsc.tws.apps.slam.streaming.msgs;

import edu.iu.dsc.tws.apps.slam.core.gridfastsalm.TNode;

import java.util.List;

public class ParticleMaps {
//    private StaticMap map;

    private TransferMap map;

    private int index;

    private int task;

    private List<TNodeValue> nodes;

    private byte []serializedMap;

    public ParticleMaps() {
    }

    public ParticleMaps(TransferMap map, int index, int task, List<TNodeValue> nodes) {
        this.map = map;
        this.index = index;
        this.task = task;
        this.nodes = nodes;
    }

    public ParticleMaps(byte[] serializedMap, int index, int task, List<TNodeValue> nodes) {
        this.index = index;
        this.task = task;
        this.nodes = nodes;
        this.serializedMap = serializedMap;
    }

    public TransferMap getMap() {
        return map;
    }

    public void setMap(TransferMap map) {
        this.map = map;
    }

    public int getIndex() {
        return index;
    }

    public int getTask() {
        return task;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setTask(int task) {
        this.task = task;
    }

    public List<TNodeValue> getNodes() {
        return nodes;
    }

    public byte[] getSerializedMap() {
        return serializedMap;
    }

    public void setSerializedMap(byte[] serializedMap) {
        this.serializedMap = serializedMap;
    }

    public void setNodes(List<TNodeValue> nodes) {
        this.nodes = nodes;
    }
}
