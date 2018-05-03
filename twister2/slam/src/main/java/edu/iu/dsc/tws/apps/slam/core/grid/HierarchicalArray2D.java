package edu.iu.dsc.tws.apps.slam.core.grid;

import edu.iu.dsc.tws.apps.slam.core.scanmatcher.PointAccumulator;
import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;

import java.util.HashSet;
import java.util.Set;

/**
 * This is where the actual map cells are kept.
 */
public class HierarchicalArray2D {
    Array2D array2D;
    Set<IntPoint> activeArea = new HashSet<IntPoint>();
    int patchMagnitude = 0;
    int patchSize;

    public HierarchicalArray2D() {
    }

    public HierarchicalArray2D(int xsize, int ysize, int patchMagnitude) {
        array2D = new Array2D((xsize >> patchMagnitude), (ysize >> patchMagnitude));

        this.patchMagnitude = patchMagnitude;
        patchSize = 1 << this.patchMagnitude;
    }

    public HierarchicalArray2D(HierarchicalArray2D hg) {
        array2D = new Array2D(hg.array2D.xsize >> hg.patchMagnitude, hg.array2D.ysize >> hg.patchMagnitude);
        assign(hg);
    }

    public int getPatchSize() {
        return patchSize;
    }

    public int getPatchMagnitude() {
        return patchMagnitude;
    }

    public Object cell(IntPoint p) {
        return cell(p.x, p.y);
    }

    public int cellState(IntPoint p) {
        return cellState(p.x, p.y);
    }

    public IntPoint patchIndexes(IntPoint p) {
        return patchIndexes(p.x, p.y);
    }

    public Set<IntPoint> getActiveArea() {
        return activeArea;
    }

    public void assign(HierarchicalArray2D hg) {
        array2D.xsize = hg.array2D.xsize;
        array2D.ysize = hg.array2D.ysize;
        array2D.cells = new Object[array2D.xsize][array2D.ysize];

        for (int x = 0; x < array2D.xsize; x++) {
            System.arraycopy(hg.array2D.cells[x], 0, array2D.cells[x], 0, array2D.ysize);
        }
        this.activeArea.clear();
        this.patchMagnitude = hg.patchMagnitude;
        this.patchSize = hg.patchSize;
    }

    public int getXSize() {
        return array2D.xsize;
    }

    public int getYSize() {
        return array2D.ysize;
    }

    public void resize(int xmin, int ymin, int xmax, int ymax) {
        int xsize = xmax - xmin;
        int ysize = ymax - ymin;
        Object[][] newcells = new Object[xsize][ysize];

        int dx = xmin < 0 ? 0 : xmin;
        int dy = ymin < 0 ? 0 : ymin;
        int Dx = xmax < this.array2D.xsize ? xmax : this.array2D.xsize;
        int Dy = ymax < this.array2D.ysize ? ymax : this.array2D.ysize;
        for (int x = dx; x < Dx; x++) {
            System.arraycopy(this.array2D.cells[x], dy, newcells[x - xmin], dy - ymin, Dy - dy);
        }
        this.array2D.cells = newcells;
        this.array2D.xsize = xsize;
        this.array2D.ysize = ysize;
    }

    public void setActiveArea(Set<IntPoint> aa, boolean patchCoords) {
        activeArea.clear();
        for (IntPoint it : aa) {
            IntPoint p;
            if (patchCoords) {
                p = it;
            } else {
                p = patchIndexes(it.x, it.y);
            }
            activeArea.add(p);
        }
    }

    public IntPoint patchIndexes(int x, int y) {
        if (x >= 0 && y >= 0) {
            return new IntPoint(x >> patchMagnitude, y >> patchMagnitude);
        }
        return new IntPoint(-1, -1);
    }

    public Array2D createPatch(IntPoint p) {
        return new Array2D(1 << patchMagnitude, 1 << patchMagnitude);
    }


    public int cellState(int x, int y) {
        IntPoint p = patchIndexes(x, y);
        if (array2D.isInside(p)) {
            if (isAllocated(p)) {
                return 0x1 | 0x2;
            } else {
                return 0x1;
            }
        }
        return 0x0;
    }

    public void allocActiveArea() {
        for (IntPoint it : activeArea) {
            Array2D ptr = (Array2D) this.array2D.cells[it.x][it.y];
            Array2D patch = null;
            if (ptr == null) {
                patch = createPatch(it);
                for (int k = 0; k < patch.xsize; k++) {
                    for (int l = 0; l < patch.ysize; l++) {
                        patch.cells[k][l] = new PointAccumulator();
                    }
                }
            } else {
                patch = createPatch(it);
                for (int k = 0; k < patch.xsize; k++) {
                    for (int l = 0; l < patch.ysize; l++) {
                        patch.cells[k][l] = new PointAccumulator((PointAccumulator) ptr.cells[k][l]);
                    }
                }
            }
            this.array2D.cells[it.x][it.y] = patch;
        }
    }

    public boolean isAllocated(int x, int y) {
        IntPoint c = patchIndexes(x, y);
        Object val = array2D.cells[c.x][c.y];
        return (val != null);
    }

    public boolean isAllocated(IntPoint patchIndexes) {
        Object val = array2D.cells[patchIndexes.x][patchIndexes.y];
        return (val != null);
    }

    public Object cell(int x, int y) {
        IntPoint c = patchIndexes(x, y);
        assert (array2D.isInside(c.x, c.y));
        if (array2D.cells[c.x][c.y] == null) {
            Array2D patch = createPatch(new IntPoint(x, y));
            for (int k = 0; k < patch.xsize; k++) {
                for (int l = 0; l < patch.ysize; l++) {
                    patch.cells[k][l] = new PointAccumulator();
                }
            }
            array2D.cells[c.x][c.y] = patch;
        }
        Array2D ptr = (Array2D) this.array2D.cells[c.x][c.y];
        return ptr.cell(new IntPoint(x - (c.x << patchMagnitude), y - (c.y << patchMagnitude)));
    }

    public Array2D getArray2D() {
        return array2D;
    }

    public void setArray2D(Array2D array2D) {
        this.array2D = array2D;
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
