package edu.iu.dsc.tws.apps.slam.core.grid;

public enum AccessibilityState {
    Outside(0x0),
    Inside(0x1),
    Allocated(0x2);

    int val;

    AccessibilityState(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }


}
